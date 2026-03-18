import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime

BASE_URL = "https://www.chiletrabajos.cl"
SEARCH_URL = BASE_URL + "/buscar"

DB_PATH = "jobs.db"


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
        pass  # duplicado por URL

    conn.close()


def scrape_page(page=1, keyword="data"):
    params = {
        "q": keyword,
        "p": page
    }

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    r = requests.get(SEARCH_URL, params=params, headers=headers)
    soup = BeautifulSoup(r.text, "html.parser")

    jobs = []

    cards = soup.select("div.card")  # estructura típica

    for card in cards:
        try:
            title_tag = card.select_one("h2 a")
            title = title_tag.text.strip()
            url = BASE_URL + title_tag["href"]

            company = card.select_one(".empresa").text.strip() if card.select_one(".empresa") else ""
            location = card.select_one(".lugar").text.strip() if card.select_one(".lugar") else ""
            date = card.select_one(".fecha").text.strip() if card.select_one(".fecha") else ""

            job = {
                "title": title,
                "company": company,
                "location": location,
                "description": "",  # lo llenamos después si queremos
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

        print(f"Saved {len(jobs)} jobs")

    print("Done.")


if __name__ == "__main__":
    run_scraper(pages=3, keyword="data")