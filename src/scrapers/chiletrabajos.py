"""
Scraper para Chiletrabajos.cl

Estrategia de corte:
--------------------
En lugar de usar un número fijo de páginas, el scraper avanza página a página
dentro de cada categoría y se detiene cuando detecta que las ofertas publicadas
superan MAX_AGE_DAYS días de antigüedad.

Esto resuelve dos problemas del diseño anterior:
1. En el primer run siempre revisaba 25 páginas aunque no hubiera nada nuevo.
2. En runs posteriores podía perderse ofertas si la ventana de 2 páginas vacías
   se activaba antes de llegar al límite de antigüedad real.

El parámetro `pages` de la interfaz estándar se usa solo como tope de seguridad
(max_pages) para evitar loops infinitos ante cambios en el sitio.
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time
import re
import sqlite3
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from src.db import init_db, get_connection

BASE_URL = "https://www.chiletrabajos.cl"

CATEGORIES = [
    "informatica",
    "medicina",
    "administracion",
    "ingenieria",
    "asistenteadministrativo",
]

PAGE_DELAY = 3
ITEMS_PER_PAGE = 30

# Número máximo de páginas por categoría como tope de seguridad.
# El corte real lo define la fecha de publicación (MAX_AGE_DAYS).
DEFAULT_MAX_PAGES = 50

# Antigüedad máxima de ofertas a considerar.
MAX_AGE_DAYS = 7


# ---------------------------------------------------------------------------
# Parseo de fechas de Chiletrabajos
# ---------------------------------------------------------------------------

def _parse_date_chiletrabajos(date_str: str) -> datetime | None:
    """
    Intenta parsear la fecha publicada por Chiletrabajos.
    El sitio usa formatos variables:
      - "Publicado: 01/04/2026"
      - "01/04/2026"
      - "Hace 3 días"
      - "Ayer"
      - "Hoy"

    Retorna un datetime o None si no se puede parsear.
    """
    if not date_str:
        return None

    text = date_str.strip().lower()

    # Normalizar prefijos comunes
    text = re.sub(r"^publicado[:\s]*", "", text).strip()

    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    if text in ("hoy", "today"):
        return today

    if text in ("ayer", "yesterday"):
        return today - timedelta(days=1)

    # "hace N días" / "N días atrás"
    m = re.search(r"hace\s+(\d+)\s+d[íi]a", text)
    if m:
        return today - timedelta(days=int(m.group(1)))

    m = re.search(r"(\d+)\s+d[íi]a[s]?\s+atr[áa]s", text)
    if m:
        return today - timedelta(days=int(m.group(1)))

    # Formato DD/MM/YYYY
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", text)
    if m:
        try:
            return datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            pass

    # Formato YYYY-MM-DD (por si acaso)
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", text)
    if m:
        try:
            return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except ValueError:
            pass

    return None


def _is_too_old(date_str: str, max_age_days: int = MAX_AGE_DAYS) -> bool:
    """
    Retorna True si la fecha parseada supera max_age_days.
    Si no se puede parsear la fecha, retorna False (conservador: no cortar).
    """
    dt = _parse_date_chiletrabajos(date_str)
    if dt is None:
        return False
    cutoff = datetime.now() - timedelta(days=max_age_days)
    return dt < cutoff


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def save_job(job):
    if not job.get("title") or not job.get("url"):
        return False
    with get_connection() as conn:
        c = conn.cursor()
        try:
            c.execute("""
            INSERT INTO jobs (title, company, location, description, url, date, source, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                job["title"], job["company"], job["location"], job["description"],
                job["url"], job["date"], job["source"], job["created_at"]
            ))
            return True
        except sqlite3.IntegrityError:
            return False


def get_existing_urls():
    with get_connection() as conn:
        c = conn.cursor()
        rows = c.execute("SELECT url FROM jobs").fetchall()
    return set(r[0] for r in rows)


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

@retry(
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
def _fetch(url, headers=None):
    return requests.get(url, headers=headers, timeout=10)


# ---------------------------------------------------------------------------
# Descripción completa
# ---------------------------------------------------------------------------

def get_job_description(url: str) -> str:
    """
    Obtiene la descripción completa desde la página individual de la oferta.
    """
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = _fetch(url, headers=headers)
        if r.status_code != 200:
            print(f"  [warn] HTTP {r.status_code} en {url}")
            return ""
    except requests.RequestException as e:
        print(f"  [error] No se pudo obtener descripción ({url}): {e}")
        return ""

    soup = BeautifulSoup(r.text, "html.parser")

    # Intento 1: buscar el h3 "Descripción oferta de trabajo"
    for h3 in soup.find_all("h3"):
        if "descripci" in h3.get_text(strip=True).lower():
            parts = []
            for sibling in h3.next_siblings:
                if sibling.name in ("h3", "h2", "h4", "hr", "table"):
                    break
                text = sibling.get_text(separator=" ", strip=True) if hasattr(sibling, "get_text") else str(sibling).strip()
                if text:
                    parts.append(text)
            result = " ".join(parts).strip()
            if result:
                return result

    # Intento 2: ID legacy
    desc = soup.select_one("#descripcion")
    if desc:
        return desc.get_text(separator=" ", strip=True)

    # Intento 3: heurística de último recurso
    main = soup.select_one("div.col-md-8, div.job-detail, article, main")
    if main:
        text = main.get_text(separator=" ", strip=True)
        if len(text) > 100:
            return text[:3000]

    return ""


# ---------------------------------------------------------------------------
# URL builder
# ---------------------------------------------------------------------------

def build_category_url(category: str, page: int) -> str:
    if page == 1:
        return f"{BASE_URL}/trabajos/{category}"
    else:
        offset = (page - 1) * ITEMS_PER_PAGE
        return f"{BASE_URL}/trabajos/{category}/{offset}"


# ---------------------------------------------------------------------------
# Scraping por página
# ---------------------------------------------------------------------------

def scrape_page(category: str, page: int = 1, existing_urls: set = None):
    """
    Scrapea una página de una categoría.

    Retorna:
        (jobs, total_found, reached_cutoff)
        - jobs: lista de dicts de ofertas nuevas parseadas
        - total_found: número de ofertas en la página (0 = sin resultados)
        - reached_cutoff: True si alguna oferta superó MAX_AGE_DAYS
          → señal para que el loop externo deje de paginar esta categoría
    """
    if existing_urls is None:
        existing_urls = get_existing_urls()

    url = build_category_url(category, page)
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        r = _fetch(url, headers=headers)
        print(f"  URL: {r.url}")
        print(f"  Status: {r.status_code}")
        if r.status_code != 200:
            return [], 0, False
    except requests.RequestException as e:
        print(f"  Request error (tras reintentos): {e}")
        return [], 0, False

    soup = BeautifulSoup(r.text, "html.parser")
    job_links = soup.select("h2 a[href*='/trabajo/']")
    total_found = len(job_links)
    print(f"  Ofertas encontradas: {total_found}")

    if not job_links:
        print("  [info] Sin resultados — fin de categoría.")
        return [], 0, False

    jobs = []
    reached_cutoff = False

    for link in job_links:
        try:
            title = link.get_text(strip=True)
            job_url = BASE_URL + link["href"] if link["href"].startswith("/") else link["href"]

            container = link.find_parent()
            while container and container.name not in ("li", "div", "article", "section"):
                container = container.find_parent()

            company, location, date_str, excerpt = "", "", "", ""

            if container:
                h3_tags = container.find_all("h3")
                company_location = h3_tags[0].get_text(strip=True) if len(h3_tags) > 0 else ""
                date_str = h3_tags[1].get_text(strip=True) if len(h3_tags) > 1 else ""

                if "," in company_location:
                    company, location = company_location.rsplit(",", 1)
                    company = company.strip()
                    location = location.strip()
                else:
                    company = company_location

                p_tags = container.find_all("p")
                excerpt = " ".join(p.get_text(strip=True) for p in p_tags if p.get_text(strip=True))

            # Corte por antigüedad: si esta oferta es más antigua que MAX_AGE_DAYS,
            # marcar reached_cutoff y dejar de procesar el resto de la página.
            # Las ofertas en Chiletrabajos están ordenadas por fecha descendente,
            # por lo que si esta supera el corte, las siguientes también lo harán.
            if _is_too_old(date_str):
                print(f"  [cutoff] Oferta '{title}' con fecha '{date_str}' supera {MAX_AGE_DAYS} días. Cortando categoría.")
                reached_cutoff = True
                break

            if job_url in existing_urls:
                print(f"  [skip] {title}")
                continue

            # Descripción completa
            full_description = get_job_description(job_url)
            existing_urls.add(job_url)
            time.sleep(1)

            description = full_description if full_description else excerpt

            if not description:
                print(f"  [warn] Sin descripción para: {title}")

            jobs.append({
                "title": title,
                "company": company,
                "location": location,
                "description": description,
                "url": job_url,
                "date": date_str,
                "source": "chiletrabajos",
                "created_at": datetime.utcnow().isoformat()
            })
            print(f"  [+] {title} — {company} {'(sin desc)' if not description else ''}")

        except Exception as e:
            print(f"  Error en oferta: {e}")
            continue

    return jobs, total_found, reached_cutoff


# ---------------------------------------------------------------------------
# Interfaz estándar del pipeline
# ---------------------------------------------------------------------------

def run_scraper(pages: int = DEFAULT_MAX_PAGES, keywords=None):
    """
    Interfaz estándar requerida por pipeline.py y load_scrapers().

    Parámetros:
        pages    — tope máximo de páginas por categoría (seguridad).
                   El corte real lo define la fecha (MAX_AGE_DAYS días).
        keywords — ignorado: Chiletrabajos usa categorías fijas, no búsqueda
                   por texto libre.

    El scraper avanza página a página dentro de cada categoría y se detiene
    en el primero de estos eventos:
        1. Se alcanza el tope `pages`.
        2. Una oferta tiene fecha > MAX_AGE_DAYS (reached_cutoff=True).
        3. La página no retorna resultados (sitio sin más páginas).
    """
    init_db()
    existing_urls = get_existing_urls()

    for category in CATEGORIES:
        print(f"\n=== Categoría: '{category}' ===")

        for page in range(1, pages + 1):
            print(f"\nScraping página {page} (tope: {pages})...")
            result, total_found, reached_cutoff = scrape_page(category, page, existing_urls)

            saved = sum(1 for job in result if save_job(job))
            print(f"Guardados: {saved} nuevos jobs (de {total_found} encontrados)")

            if reached_cutoff:
                print(f"  [stop] Corte por antigüedad (>{MAX_AGE_DAYS} días). Siguiente categoría.")
                break

            if total_found == 0:
                print("  [stop] Página sin resultados. Siguiente categoría.")
                break

            if page < pages:
                print(f"  Esperando {PAGE_DELAY}s antes de la siguiente página...")
                time.sleep(PAGE_DELAY)

    print("\nDone.")


if __name__ == "__main__":
    run_scraper()