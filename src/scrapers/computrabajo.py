"""
Scraper para Computrabajo Chile (cl.computrabajo.com)

A diferencia de Chiletrabajos (categorías fijas), Computrabajo busca por
texto libre. Por eso SÍ usa `keywords` — una búsqueda HTTP por cada
título deseado en config/desired_titles.json.

Selectores confirmados contra un scraper de Computrabajo en producción
(mismo template en todos los países del grupo, co./cl./mx.computrabajo.com):
    - Contenedor por oferta: <article class="box_offer">
    - Título + link:         <h2><a>...</a></h2>
    - Empresa:                <a offer-grid-article-company-url="...">
    - Ubicación:               <p class="fs16"> (sin clase dFlex) > <span class="mr10">
    - Fecha:                   <p class="fs13 fc_aux mt15">
    - Paginación:               ?p=<n>

Igual que Chiletrabajos: NO se visita la página de detalle de cada oferta.
Computrabajo sí ofrece descripción completa ahí, pero el proyecto ya
decidió que no la necesita (el filtro opera solo sobre el título) y
visitar cada detalle multiplicaría las requests innecesariamente.

Estrategia de corte por antigüedad: igual que Chiletrabajos, MAX_AGE_DAYS
días vía fecha relativa ("Hace N días", "Ayer", "Hoy"). Computrabajo no
suele mostrar fechas absolutas para ofertas recientes, así que ese es el
caso que se cubre; si aparece un formato no reconocido, es conservador
(no corta).
"""

import re
import time
import unicodedata
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from src.db import init_db
from src.models import JobDict
from src.scrapers._common import save_job, get_existing_urls, get_headers

BASE_URL = "https://cl.computrabajo.com"

PAGE_DELAY = 3
KEYWORD_DELAY = 2  # pausa extra entre búsquedas de distintos títulos

DEFAULT_MAX_PAGES = 10  # tope de seguridad POR keyword, no global
MAX_AGE_DAYS = 7


# ---------------------------------------------------------------------------
# Parseo de fechas de Computrabajo (formatos relativos: "Hace N días", "Ayer", "Hoy")
# ---------------------------------------------------------------------------

def _parse_date_computrabajo(date_str: str) -> datetime | None:
    if not date_str:
        return None

    text = date_str.strip().lower()
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    if text in ("hoy", "today"):
        return today

    if text in ("ayer", "yesterday"):
        return today - timedelta(days=1)

    m = re.search(r"hace\s+(\d+)\s+d[íi]a", text)
    if m:
        return today - timedelta(days=int(m.group(1)))

    m = re.search(r"hace\s+(\d+)\s+hora", text)
    if m:
        return today  # menos de un día, cuenta como hoy

    # Formato DD/MM/YYYY, por si aparece en ofertas más antiguas
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", text)
    if m:
        try:
            return datetime(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except ValueError:
            pass

    return None


def _is_too_old(date_str: str, max_age_days: int = MAX_AGE_DAYS) -> bool:
    dt = _parse_date_computrabajo(date_str)
    if dt is None:
        return False  # conservador: si no se puede parsear, no cortar
    cutoff = datetime.now() - timedelta(days=max_age_days)
    return dt < cutoff


# ---------------------------------------------------------------------------
# Normalización de término de búsqueda -> slug de URL
# ---------------------------------------------------------------------------

def _slugify(term: str) -> str:
    """
    'informática en salud' -> 'informatica-en-salud'
    Computrabajo arma sus URLs de búsqueda como /trabajo-de-<slug>.
    """
    text = term.strip().lower()
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = re.sub(r"[^a-z0-9\s-]", "", text)
    text = re.sub(r"\s+", "-", text)
    return text


def build_search_url(term: str, page: int = 1) -> str:
    slug = _slugify(term)
    url = f"{BASE_URL}/trabajo-de-{slug}"
    if page > 1:
        url += f"?p={page}"
    return url


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
    return requests.get(url, headers=headers, timeout=15)


# ---------------------------------------------------------------------------
# Scraping por página
# ---------------------------------------------------------------------------

def scrape_page(term: str, page: int, existing_urls: set):
    """
    Retorna (jobs, total_found, reached_cutoff) — misma interfaz que
    chiletrabajos.scrape_page() por consistencia.
    """
    url = build_search_url(term, page)
    headers = get_headers()

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
    offers = soup.find_all("article", class_="box_offer")
    total_found = len(offers)
    print(f"  Ofertas encontradas: {total_found}")

    if not offers:
        return [], 0, False

    jobs: list[JobDict] = []
    reached_cutoff = False

    for offer in offers:
        try:
            h2 = offer.find("h2")
            title_link = h2.find("a") if h2 else None
            if not title_link:
                continue
            title = title_link.get_text(strip=True)

            link_elem = offer.find("a", class_="js-o-link")
            href = link_elem.get("href") if link_elem else title_link.get("href")
            if not href:
                continue
            job_url = href if href.startswith("http") else BASE_URL + href

            company_elem = offer.find("a", attrs={"offer-grid-article-company-url": True})
            company = company_elem.get_text(strip=True) if company_elem else ""

            location = ""
            for p in offer.find_all("p", class_="fs16"):
                if "dFlex" in (p.get("class") or []):
                    continue
                loc_span = p.find("span", class_="mr10")
                if loc_span:
                    location = loc_span.get_text(strip=True)
                    break

            date_elem = offer.find("p", class_="fs13 fc_aux mt15")
            date_str = date_elem.get_text(strip=True) if date_elem else ""

            # Corte por antigüedad. Igual que Chiletrabajos: las ofertas
            # vienen ordenadas por fecha descendente, así que cortamos
            # la página entera apenas aparece la primera muy antigua.
            if _is_too_old(date_str):
                print(f"  [cutoff] Oferta '{title}' con fecha '{date_str}' supera {MAX_AGE_DAYS} días. Cortando búsqueda.")
                reached_cutoff = True
                break

            if job_url in existing_urls:
                print(f"  [skip] {title}")
                continue

            existing_urls.add(job_url)

            jobs.append(JobDict(
                title=title,
                company=company,
                location=location,
                description="",  # sin fetch de detalle, ver docstring del módulo
                url=job_url,
                date=date_str,
                source="computrabajo",
                created_at=datetime.utcnow().isoformat(),
            ))
            print(f"  [+] {title} — {company}")

        except Exception as e:
            print(f"  Error en oferta: {e}")
            continue

    return jobs, total_found, reached_cutoff


# ---------------------------------------------------------------------------
# Interfaz estándar del pipeline
# ---------------------------------------------------------------------------

def run_scraper(pages: int = DEFAULT_MAX_PAGES, keywords=None):
    """
    A diferencia de Chiletrabajos, Computrabajo SÍ usa `keywords`: hace
    una búsqueda independiente por cada título en desired_titles.json.

    Nota de volumen: con ~19 títulos deseados, esto son ~19+ requests
    mínimo por run (más si hay paginación). Cada búsqueda es independiente
    y se detiene por su propio corte de 7 días o por `pages`, lo que
    ocurra primero — normalmente mucho antes de llegar a `pages`.
    """
    if not keywords:
        print("[computrabajo] Sin keywords (desired_titles.json vacío o no cargado). Nada que buscar.")
        return

    init_db()
    existing_urls = get_existing_urls()

    for i, term in enumerate(keywords):
        print(f"\n=== Búsqueda: '{term}' ===")

        for page in range(1, pages + 1):
            print(f"\nScraping página {page} (tope: {pages})...")
            result, total_found, reached_cutoff = scrape_page(term, page, existing_urls)

            saved = sum(1 for job in result if save_job(job))
            print(f"Guardados: {saved} nuevos jobs (de {total_found} encontrados)")

            if reached_cutoff:
                print(f"  [stop] Corte por antigüedad (>{MAX_AGE_DAYS} días). Siguiente término.")
                break

            if total_found == 0:
                print("  [stop] Página sin resultados. Siguiente término.")
                break

            if page < pages:
                print(f"  Esperando {PAGE_DELAY}s antes de la siguiente página...")
                time.sleep(PAGE_DELAY)

        if i < len(keywords) - 1:
            time.sleep(KEYWORD_DELAY)

    print("\nDone.")


if __name__ == "__main__":
    # Permite correrlo solo, usando los mismos desired_titles.json del proyecto
    import json
    with open("config/desired_titles.json", "r", encoding="utf-8") as f:
        titles = json.load(f).get("desired_titles", [])
    run_scraper(keywords=titles)