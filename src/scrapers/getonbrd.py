"""
Scraper para GetOnBoard (getonbrd.com) vía su API pública oficial.

A diferencia de Chiletrabajos y Computrabajo, este NO hace HTML scraping.
GetOnBoard expone una API pública sin autenticación para navegar los
mismos datos visibles en la web (confirmado en el código fuente de su
librería oficial en Ruby: getonbrd/getonbrd-ruby en GitHub — el header
Authorization solo se agrega si hay una api_key configurada, y las
clases bajo Public:: nunca la requieren).

Base URL y endpoints confirmados desde el código fuente oficial:
    https://www.getonbrd.com/api/v0
    GET /search/jobs?query=<texto>   (búsqueda de texto libre, min. 3 caracteres)
    GET /jobs                         (listado general)
    GET /categories                   (categorías)

--------------------------------------------------------------------------
AVISO IMPORTANTE — leer antes de correr esto por primera vez
--------------------------------------------------------------------------
Confirmé que la API es real, pública y no requiere autenticación. NO pude
confirmar en vivo los nombres exactos de los campos dentro de "attributes"
de cada job (título, empresa, url, fecha) — no tengo acceso de red a
getonbrd.com desde mi entorno de pruebas. Por eso:

  1. La extracción de campos prueba varios nombres candidatos por campo
     (ver _extract_job más abajo), basados en la librería oficial y en
     dos proyectos de terceros que documentan la forma típica de estos
     datos (remote, remote_modality, company.name, etc.)
  2. Si en el primer run casi ningún job se logra extraer bien (título o
     url faltantes), el scraper imprime el JSON crudo del primer item sin
     procesar — pégamelo y ajusto los nombres de campo en un minuto.
  3. Hasta confirmar esto, trata los resultados de este scraper con más
     escepticismo que los de Chiletrabajos/Computrabajo (esos sí están
     verificados contra selectores reales).
--------------------------------------------------------------------------
"""

import time
from datetime import datetime, timedelta

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from src.db import init_db
from src.models import JobDict
from src.scrapers._common import save_job, get_existing_urls

BASE_URL = "https://www.getonbrd.com/api/v0"

REQUEST_DELAY = 2  # pausa entre búsquedas de distintos keywords
DEFAULT_MAX_PAGES = 5  # tope de seguridad POR keyword
PER_PAGE = 25
MAX_AGE_DAYS = 7

# Cuántos jobs sin título/url válidos toleramos antes de asumir que el
# mapeo de campos está mal y mostrar el diagnóstico completo.
DIAGNOSTIC_THRESHOLD = 0.5


@retry(
    retry=retry_if_exception_type((requests.ConnectionError, requests.Timeout)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
def _fetch_json(url, params=None):
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------------------
# Extracción defensiva de campos (ver aviso en el docstring del módulo)
# ---------------------------------------------------------------------------

def _first_present(d: dict, *keys, default=""):
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return default


def _extract_company_name(attrs: dict) -> str:
    # Caso simple: campo plano
    flat = _first_present(attrs, "hiring_company", "company_name", "hiring_organization")
    if flat:
        return flat

    # Caso JSON:API expandido: attrs["company"]["data"]["attributes"]["name"]
    company = attrs.get("company")
    if isinstance(company, dict):
        data = company.get("data", company)
        if isinstance(data, dict):
            inner_attrs = data.get("attributes", data)
            name = inner_attrs.get("name") if isinstance(inner_attrs, dict) else None
            if name:
                return name
    return ""


def _extract_url(attrs: dict, job_id) -> str:
    direct = _first_present(attrs, "url", "permalink", "public_url", "link")
    if direct:
        return direct if direct.startswith("http") else f"https://www.getonbrd.com{direct}"

    slug = _first_present(attrs, "slug", default=None) or job_id
    category = attrs.get("category")
    category_slug = None
    if isinstance(category, dict):
        data = category.get("data", category)
        if isinstance(data, dict):
            inner = data.get("attributes", data)
            category_slug = inner.get("slug") if isinstance(inner, dict) else data.get("id")

    if slug and category_slug:
        return f"https://www.getonbrd.com/jobs/{category_slug}/{slug}"
    if slug:
        # Fallback menos preciso pero funcional en la mayoría de sitios Rails:
        # suele redirigir al slug correcto si existe una ruta corta.
        return f"https://www.getonbrd.com/jobs/{slug}"
    return ""


def _extract_location(attrs: dict) -> str:
    if attrs.get("remote") is True:
        zone = _first_present(attrs, "remote_zone", "remote_modality")
        return f"Remoto ({zone})" if zone else "Remoto"
    return _first_present(attrs, "city", "location", "place")


def _extract_date(attrs: dict):
    raw = _first_present(attrs, "published_at", "created_at", "posted_at", "date", default=None)
    return raw


def _parse_iso_date(raw) -> datetime | None:
    if not raw or not isinstance(raw, str):
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def _is_too_old(raw_date, max_age_days: int = MAX_AGE_DAYS) -> bool:
    dt = _parse_iso_date(raw_date)
    if dt is None:
        return False  # conservador: si no se puede parsear, no cortar
    cutoff = datetime.now() - timedelta(days=max_age_days)
    return dt < cutoff


def _extract_job(raw_item: dict) -> JobDict | None:
    # Soporta tanto forma JSON:API ({"id","type","attributes":{...}})
    # como forma plana (los campos directo en raw_item), por si acaso.
    attrs = raw_item.get("attributes", raw_item)
    job_id = raw_item.get("id")

    title = _first_present(attrs, "title", "job_title", "name")
    url = _extract_url(attrs, job_id)

    if not title or not url:
        return None

    return JobDict(
        title=title,
        company=_extract_company_name(attrs),
        location=_extract_location(attrs),
        description="",
        url=url,
        date=str(_extract_date(attrs) or ""),
        source="getonbrd",
        created_at=datetime.utcnow().isoformat(),
    ), _extract_date(attrs)


def _print_diagnostic(raw_item: dict):
    import json
    print("\n" + "=" * 70)
    print("[getonbrd] DIAGNÓSTICO: no se pudo extraer título/url de la mayoría")
    print("de los resultados. JSON crudo del primer item sin procesar:")
    print(json.dumps(raw_item, indent=2, ensure_ascii=False)[:3000])
    print("=" * 70 + "\n")


# ---------------------------------------------------------------------------
# Interfaz estándar del pipeline
# ---------------------------------------------------------------------------

def run_scraper(pages: int = DEFAULT_MAX_PAGES, keywords=None):
    if not keywords:
        print("[getonbrd] Sin keywords (desired_titles.json vacío o no cargado). Nada que buscar.")
        return

    init_db()
    existing_urls = get_existing_urls()
    diagnostic_shown = False

    for i, term in enumerate(keywords):
        if len(term) < 3:
            print(f"[getonbrd] Saltando '{term}': la búsqueda requiere mínimo 3 caracteres.")
            continue

        print(f"\n=== Búsqueda: '{term}' ===")

        for page in range(1, pages + 1):
            params = {"query": term, "per_page": PER_PAGE, "page": page, "lang": "es"}
            try:
                response = _fetch_json(f"{BASE_URL}/search/jobs", params=params)
            except requests.RequestException as e:
                print(f"  Request error (tras reintentos): {e}")
                break

            raw_items = response.get("data", response if isinstance(response, list) else [])
            total_found = len(raw_items)
            print(f"  Página {page}: {total_found} resultados crudos")

            if total_found == 0:
                break

            extracted_ok = 0
            saved = 0
            reached_cutoff = False

            for raw_item in raw_items:
                result = _extract_job(raw_item)
                if result is None:
                    continue
                extracted_ok += 1
                job, raw_date = result

                if _is_too_old(raw_date):
                    reached_cutoff = True
                    continue

                if job["url"] in existing_urls:
                    continue
                existing_urls.add(job["url"])

                if save_job(job):
                    saved += 1
                    print(f"  [+] {job['title']} — {job['company']}")

            print(f"  Extraídos correctamente: {extracted_ok}/{total_found} | Guardados: {saved}")

            if not diagnostic_shown and total_found > 0 and (extracted_ok / total_found) < DIAGNOSTIC_THRESHOLD:
                _print_diagnostic(raw_items[0])
                diagnostic_shown = True  # solo una vez por corrida, no inundar el log

            if reached_cutoff:
                print(f"  [stop] Corte por antigüedad (>{MAX_AGE_DAYS} días). Siguiente término.")
                break
            if total_found < PER_PAGE:
                break  # última página

        if i < len(keywords) - 1:
            time.sleep(REQUEST_DELAY)

    print("\nDone.")


if __name__ == "__main__":
    import json
    with open("config/desired_titles.json", "r", encoding="utf-8") as f:
        titles = json.load(f).get("desired_titles", [])
    run_scraper(keywords=titles)