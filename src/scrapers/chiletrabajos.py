import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
import time

BASE_URL = "https://www.chiletrabajos.cl"
SEARCH_URL = BASE_URL + "/encuentra-un-empleo"
DB_PATH = "data/jobs.db"
PAGE_SIZE = 30

# Delay entre páginas para evitar rate limiting (segundos)
PAGE_DELAY = 3


def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        company TEXT,
        location TEXT,
        description TEXT,
        url TEXT UNIQUE,
        date TEXT,
        source TEXT,
        created_at TEXT
    )
    """)
    conn.commit()
    conn.close()


def save_job(job):
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


def get_job_description(url):
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code != 200:
            return ""
    except requests.RequestException:
        return ""
    soup = BeautifulSoup(r.text, "html.parser")
    desc = soup.select_one("#descripcion")
    return desc.get_text(separator=" ", strip=True) if desc else ""


def build_page_url(page, keyword):
    """
    Construye la URL correcta para cada página.

    chiletrabajos usa query params para búsqueda y paginación:
      Página 1: /encuentra-un-empleo?Busqueda=data
      Página 2: /encuentra-un-empleo?Busqueda=data&pagina=2
      Página N: /encuentra-un-empleo?Busqueda=data&pagina=N

    Si el sitio cambia su esquema de paginación, solo hay que
    actualizar esta función.
    """
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
        r = requests.get(url, params=params, headers=headers, timeout=10)
        print(f"  URL: {r.url}")
        print(f"  Status: {r.status_code}")
        if r.status_code != 200:
            return []
    except requests.RequestException as e:
        print(f"  Request error: {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    job_links = soup.select("h2 a[href*='/trabajo/']")
    print(f"  Ofertas encontradas: {len(job_links)}")

    # Si no hay resultados en esta página, detenemos el scraping anticipadamente
    if not job_links:
        print("  [info] Sin resultados — probablemente última página alcanzada.")
        return None  # None indica "detener", [] indica "página vacía pero continuar"

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


def run_scraper(pages=2, keyword="data"):
    init_db()
    existing_urls = get_existing_urls()

    for page in range(1, pages + 1):
        print(f"\nScraping página {page}...")
        result = scrape_page(page, keyword, existing_urls)

        # None significa que no hubo resultados → se detiene el loop anticipadamente
        if result is None:
            print("No hay más páginas con resultados. Deteniendo scraper.")
            break

        for job in result:
            save_job(job)
        print(f"Guardados: {len(result)} nuevos jobs")

        # Delay entre páginas para evitar rate limiting
        if page < pages:
            print(f"  Esperando {PAGE_DELAY}s antes de la siguiente página...")
            time.sleep(PAGE_DELAY)

    print("\nDone.")


if __name__ == "__main__":
    run_scraper(pages=2, keyword="data")