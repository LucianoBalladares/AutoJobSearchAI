import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
import sqlite3
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from src.db import init_db, get_connection

BASE_URL = "https://www.chiletrabajos.cl"

# Categorías relevantes para el perfil (Health Informatics + Data Analytics).
# La paginación usa offset de 30 en 30:
#   Página 1 → /trabajos/{categoria}
#   Página 2 → /trabajos/{categoria}/30
#   Página 3 → /trabajos/{categoria}/60   ... etc.
CATEGORIES = [
    "informatica",              # Informática / Telecomunicaciones
    "medicina",                 # Medicina / Salud
    "administracion",           # Administración (puede tener roles de analista/BI)
    "ingenieria",               # Profesionales y Técnicos
    "asistenteadministrativo",  # A veces aparecen roles de datos aquí
]

PAGE_DELAY = 3
ITEMS_PER_PAGE = 30

# Si en N páginas consecutivas no se guarda ningún job nuevo, asumimos
# que el sitio está repitiendo resultados y paramos esa categoría.
MAX_EMPTY_PAGES = 2


def save_job(job):
    if not job.get("title") or not job.get("url"):
        print(f"  [skip] Oferta sin título o URL, descartada.")
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
            return True  # job nuevo guardado
        except sqlite3.IntegrityError:
            return False  # URL duplicada — ya existe


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
    desc = soup.select_one("#descripcion")
    return desc.get_text(separator=" ", strip=True) if desc else ""


def build_category_url(category: str, page: int) -> str:
    """
    Construye la URL correcta según la estructura real de Chiletrabajos:
      Página 1 → /trabajos/{categoria}
      Página 2 → /trabajos/{categoria}/30
      Página 3 → /trabajos/{categoria}/60
    """
    if page == 1:
        return f"{BASE_URL}/trabajos/{category}"
    else:
        offset = (page - 1) * ITEMS_PER_PAGE
        return f"{BASE_URL}/trabajos/{category}/{offset}"


def scrape_page(category: str, page: int = 1, existing_urls: set = None):
    """
    Scrapea una página de una categoría.
    Retorna (jobs_nuevos, total_encontrados).
    Retorna (None, 0) si la página no tiene resultados.
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

            container = link.find_parent()
            while container and container.name not in ("li", "div", "article", "section"):
                container = container.find_parent()

            if not container:
                print(f"  [warn] No se encontró contenedor para: {title}")
                continue

            h3_tags = container.find_all("h3")
            company_location = h3_tags[0].get_text(strip=True) if len(h3_tags) > 0 else ""
            date = h3_tags[1].get_text(strip=True) if len(h3_tags) > 1 else ""

            if "," in company_location:
                company, location = company_location.rsplit(",", 1)
                company = company.strip()
                location = location.strip()
            else:
                company = company_location
                location = ""

            description = get_job_description(job_url)
            existing_urls.add(job_url)
            time.sleep(1)

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
            print(f"  [+] {title} — {company}")

        except Exception as e:
            print(f"  Error en oferta: {e}")
            continue

    return jobs, total_found


def run_scraper(pages=2, keywords=None):
    """
    Interfaz estándar del pipeline. El parámetro `keywords` se ignora porque
    Chiletrabajos no usa búsqueda por texto libre — opera por categorías fijas.

    Early-stop por categoría: si MAX_EMPTY_PAGES páginas consecutivas no
    aportan ningún job nuevo, asumimos que el sitio está repitiendo y paramos.
    Esto evita desperdiciar tiempo en páginas fantasma como las de
    asistenteadministrativo pasada la página 15.
    """
    init_db()
    existing_urls = get_existing_urls()

    for category in CATEGORIES:
        print(f"\n=== Categoría: '{category}' ===")
        empty_streak = 0  # páginas consecutivas sin jobs nuevos

        for page in range(1, pages + 1):
            print(f"\nScraping página {page}...")
            result, total_found = scrape_page(category, page, existing_urls)

            # Página sin resultados → fin real de la categoría
            if result is None:
                print("No hay más páginas. Siguiente categoría.")
                break

            saved = sum(1 for job in result if save_job(job))
            print(f"Guardados: {saved} nuevos jobs (de {total_found} encontrados)")

            if saved == 0:
                empty_streak += 1
                print(f"  [warn] Página vacía ({empty_streak}/{MAX_EMPTY_PAGES}). "
                      f"El sitio puede estar repitiendo resultados.")
                if empty_streak >= MAX_EMPTY_PAGES:
                    print(f"  [stop] {MAX_EMPTY_PAGES} páginas seguidas sin jobs nuevos. "
                          f"Saltando a la siguiente categoría.")
                    break
            else:
                empty_streak = 0  # reset si encontramos algo nuevo

            if page < pages:
                print(f"  Esperando {PAGE_DELAY}s antes de la siguiente página...")
                time.sleep(PAGE_DELAY)

    print("\nDone.")


if __name__ == "__main__":
    run_scraper(pages=5)