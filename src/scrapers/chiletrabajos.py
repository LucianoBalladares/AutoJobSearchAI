import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
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
MAX_EMPTY_PAGES = 2


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


@retry(
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
def _fetch(url, headers=None):
    return requests.get(url, headers=headers, timeout=10)


def get_job_description(url: str) -> str:
    """
    Obtiene la descripción completa desde la página individual de la oferta.

    La descripción en Chiletrabajos NO está en #descripcion — está en el
    bloque de texto que sigue al h3 'Descripción oferta de trabajo'.
    Estrategia: buscar todos los párrafos/texto dentro del contenedor
    principal de la oferta, excluyendo navegación y widgets laterales.
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

    # Intento 1: buscar el h3 que dice "Descripción oferta de trabajo"
    # y recolectar el texto que sigue hasta el próximo h3/hr.
    for h3 in soup.find_all("h3"):
        if "descripci" in h3.get_text(strip=True).lower():
            # Recolectar siblings de texto hasta el próximo bloque estructural
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

    # Intento 2: buscar por ID legacy (puede existir en algunas ofertas)
    desc = soup.select_one("#descripcion")
    if desc:
        return desc.get_text(separator=" ", strip=True)

    # Intento 3: buscar el div/section con más texto en el área central
    # (heurística de último recurso)
    main = soup.select_one("div.col-md-8, div.job-detail, article, main")
    if main:
        text = main.get_text(separator=" ", strip=True)
        if len(text) > 100:
            return text[:3000]  # cap para no guardar páginas enteras

    return ""


def build_category_url(category: str, page: int) -> str:
    if page == 1:
        return f"{BASE_URL}/trabajos/{category}"
    else:
        offset = (page - 1) * ITEMS_PER_PAGE
        return f"{BASE_URL}/trabajos/{category}/{offset}"


def scrape_page(category: str, page: int = 1, existing_urls: set = None):
    if existing_urls is None:
        existing_urls = get_existing_urls()

    url = build_category_url(category, page)
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        r = _fetch(url, headers=headers)
        print(f"  URL: {r.url}")
        print(f"  Status: {r.status_code}")
        if r.status_code != 200:
            return [], 0
    except requests.RequestException as e:
        print(f"  Request error (tras reintentos): {e}")
        return [], 0

    soup = BeautifulSoup(r.text, "html.parser")
    job_links = soup.select("h2 a[href*='/trabajo/']")
    total_found = len(job_links)
    print(f"  Ofertas encontradas: {total_found}")

    if not job_links:
        print("  [info] Sin resultados — fin de categoría.")
        return None, 0

    jobs = []

    for link in job_links:
        try:
            title = link.get_text(strip=True)
            job_url = BASE_URL + link["href"] if link["href"].startswith("/") else link["href"]

            if job_url in existing_urls:
                print(f"  [skip] {title}")
                continue

            # Buscar el contenedor del listado para extraer empresa, fecha
            # y el extracto de descripción ya disponible en la lista.
            container = link.find_parent()
            while container and container.name not in ("li", "div", "article", "section"):
                container = container.find_parent()

            company, location, date, excerpt = "", "", "", ""

            if container:
                h3_tags = container.find_all("h3")
                company_location = h3_tags[0].get_text(strip=True) if len(h3_tags) > 0 else ""
                date = h3_tags[1].get_text(strip=True) if len(h3_tags) > 1 else ""

                if "," in company_location:
                    company, location = company_location.rsplit(",", 1)
                    company = company.strip()
                    location = location.strip()
                else:
                    company = company_location

                # El párrafo/texto corto del listado ya tiene un buen resumen.
                # Lo usamos como descripción base y lo complementamos con
                # el texto completo de la página individual.
                p_tags = container.find_all("p")
                excerpt = " ".join(p.get_text(strip=True) for p in p_tags if p.get_text(strip=True))

            # Obtener descripción completa desde la página de la oferta.
            full_description = get_job_description(job_url)
            existing_urls.add(job_url)
            time.sleep(1)

            # Usar descripción completa si existe; fallback al extracto del listado.
            description = full_description if full_description else excerpt

            if not description:
                print(f"  [warn] Sin descripción para: {title}")

            jobs.append({
                "title": title,
                "company": company,
                "location": location,
                "description": description,
                "url": job_url,
                "date": date,
                "source": "chiletrabajos",
                "created_at": datetime.utcnow().isoformat()
            })
            print(f"  [+] {title} — {company} {'(sin desc)' if not description else ''}")

        except Exception as e:
            print(f"  Error en oferta: {e}")
            continue

    return jobs, total_found


def run_scraper(pages=2, keywords=None):
    """
    Interfaz estándar del pipeline. keywords se ignora — Chiletrabajos
    usa categorías fijas, no búsqueda por texto libre.
    """
    init_db()
    existing_urls = get_existing_urls()

    for category in CATEGORIES:
        print(f"\n=== Categoría: '{category}' ===")
        empty_streak = 0

        for page in range(1, pages + 1):
            print(f"\nScraping página {page}...")
            result, total_found = scrape_page(category, page, existing_urls)

            if result is None:
                print("No hay más páginas. Siguiente categoría.")
                break

            saved = sum(1 for job in result if save_job(job))
            print(f"Guardados: {saved} nuevos jobs (de {total_found} encontrados)")

            if saved == 0:
                empty_streak += 1
                print(f"  [warn] Página vacía ({empty_streak}/{MAX_EMPTY_PAGES}).")
                if empty_streak >= MAX_EMPTY_PAGES:
                    print(f"  [stop] {MAX_EMPTY_PAGES} páginas seguidas sin jobs nuevos. Siguiente categoría.")
                    break
            else:
                empty_streak = 0

            if page < pages:
                print(f"  Esperando {PAGE_DELAY}s antes de la siguiente página...")
                time.sleep(PAGE_DELAY)

    print("\nDone.")


if __name__ == "__main__":
    run_scraper(pages=5)