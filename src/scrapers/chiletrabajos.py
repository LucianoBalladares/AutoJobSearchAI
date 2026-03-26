import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
import time
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from src.db import init_db, DB_PATH

BASE_URL = "https://www.chiletrabajos.cl"
SEARCH_URL = BASE_URL + "/encuentra-un-empleo"
PAGE_SIZE = 30
PAGE_DELAY = 3


def save_job(job):
    # Valida campos obligatorios antes de insertar
    if not job.get("title") or not job.get("url"):
        print(f"  [skip] Oferta sin título o URL, descartada.")
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute("""
        INSERT INTO jobs (title, company, location, description, url, date, source, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            job["title"], job["company"], job["location"], job["description"],
            job["url"], job["date"], job["source"], job["created_at"]
        ))
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    conn.close()


def get_existing_urls():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    rows = c.execute("SELECT url FROM jobs").fetchall()
    conn.close()
    return set(r[0] for r in rows)


@retry(
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
def _fetch(url, params=None, headers=None):
    """
    GET con retry automático en errores de red y timeouts.
    Reintentos: hasta 3 veces, con backoff exponencial (2s → 4s → 8s).
    Solo reintenta en errores de conexión/timeout, no en errores HTTP (4xx/5xx).
    """
    return requests.get(url, params=params, headers=headers, timeout=10)


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


def build_page_url(page, keyword):
    params = {}
    if keyword:
        params["Busqueda"] = keyword
    if page > 1:
        params["pagina"] = page
    return SEARCH_URL, params


def scrape_page(page=1, keyword="data", existing_urls=None):
    if existing_urls is None:
        existing_urls = get_existing_urls()

    url, params = build_page_url(page, keyword)
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        r = _fetch(url, params=params, headers=headers)
        print(f"  URL: {r.url}")
        print(f"  Status: {r.status_code}")
        if r.status_code != 200:
            return []
    except requests.RequestException as e:
        print(f"  Request error (tras reintentos): {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    job_links = soup.select("h2 a[href*='/trabajo/']")
    print(f"  Ofertas encontradas: {len(job_links)}")

    if not job_links:
        print("  [info] Sin resultados — probablemente última página alcanzada.")
        return None

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

    return jobs


def run_scraper(pages=2, keywords=None):
    """
    Ejecuta el scraper para una lista de keywords.
    Si no se pasan keywords, usa ["data"] como fallback.
    Deduplica automáticamente — una URL ya vista no se vuelve a procesar
    aunque aparezca en múltiples búsquedas de keywords distintas.
    """
    if keywords is None:
        keywords = ["data"]

    init_db()
    existing_urls = get_existing_urls()

    for keyword in keywords:
        print(f"\n=== Keyword: '{keyword}' ===")
        for page in range(1, pages + 1):
            print(f"\nScraping página {page}...")
            result = scrape_page(page, keyword, existing_urls)

            if result is None:
                print("No hay más páginas con resultados. Siguiente keyword.")
                break

            for job in result:
                save_job(job)
            print(f"Guardados: {len(result)} nuevos jobs")

            if page < pages:
                print(f"  Esperando {PAGE_DELAY}s antes de la siguiente página...")
                time.sleep(PAGE_DELAY)

    print("\nDone.")


if __name__ == "__main__":
    run_scraper(pages=2, keywords=["data", "salud", "analista"])