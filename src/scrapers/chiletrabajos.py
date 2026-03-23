import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
import time

BASE_URL = "https://www.chiletrabajos.cl"
SEARCH_URL = BASE_URL + "/buscar"
DB_PATH = "data/jobs.db"


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
            job["title"],
            job["company"],
            job["location"],
            job["description"],
            job["url"],
            job["date"],
            job["source"],
            job["created_at"]
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

    if desc:
        return desc.get_text(separator=" ", strip=True)

    return ""


def scrape_page(page=1, keyword="data"):
    params = {
        "q": keyword,
        "p": page
    }

    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        r = requests.get(SEARCH_URL, params=params, headers=headers, timeout=10)
        if r.status_code != 200:
            return []
    except requests.RequestException:
        return []

    soup = BeautifulSoup(r.text, "html.parser")

    jobs = []
    cards = soup.select("div.card")

    existing_urls = get_existing_urls()

    for card in cards:
        try:
            title_tag = card.select_one("h2 a")
            if not title_tag:
                continue

            title = title_tag.text.strip()
            url = BASE_URL + title_tag["href"]

            # skip duplicados
            if url in existing_urls:
                continue

            company = card.select_one(".empresa").text.strip() if card.select_one(".empresa") else ""
            location = card.select_one(".lugar").text.strip() if card.select_one(".lugar") else ""
            date = card.select_one(".fecha").text.strip() if card.select_one(".fecha") else ""

            # descripción (solo si es nuevo)
            description = get_job_description(url)
            time.sleep(1)

            job = {
                "title": title,
                "company": company,
                "location": location,
                "description": description,
                "url": url,
                "date": date,
                "source": "chiletrabajos",
                "created_at": datetime.utcnow().isoformat()
            }

            jobs.append(job)

        except Exception:
            continue

    return jobs


def run_scraper(pages=2, keyword="data"):
    init_db()

    for page in range(1, pages + 1):
        print(f"Scraping page {page}...")
        jobs = scrape_page(page, keyword)

        for job in jobs:
            save_job(job)

        print(f"Saved {len(jobs)} new jobs")

    print("Done.")


if __name__ == "__main__":
    run_scraper(pages=2, keyword="data")