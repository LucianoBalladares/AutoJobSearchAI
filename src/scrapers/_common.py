"""
Helpers compartidos entre scrapers.

Empieza a existir porque con 2+ scrapers, save_job() y get_existing_urls()
se duplicaban idénticos en cada archivo. Este módulo NO exporta
run_scraper(), así que load_scrapers() lo ignora automáticamente (ver
convención de nombres con guion bajo en src/scrapers/__init__.py).
"""

import random
import sqlite3

from src.db import get_connection
from src.models import JobDict

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]


def get_headers(extra: dict | None = None) -> dict:
    """
    Headers con User-Agent rotativo. Un UA fijo (como el que usaba
    chiletrabajos.py originalmente) es una señal fácil de detectar para
    sitios con más tráfico/monitoreo que Chiletrabajos.
    """
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-CL,es;q=0.9,en;q=0.8",
    }
    if extra:
        headers.update(extra)
    return headers


def save_job(job: JobDict) -> bool:
    """
    Inserta un job en la DB. Retorna False silenciosamente si la URL
    ya existe (UNIQUE constraint) — es el mecanismo de dedup del proyecto,
    no un error.
    """
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


def get_existing_urls() -> set:
    """Todas las URLs ya guardadas, sin importar la fuente (dedup cross-scraper)."""
    with get_connection() as conn:
        c = conn.cursor()
        rows = c.execute("SELECT url FROM jobs").fetchall()
    return set(r[0] for r in rows)