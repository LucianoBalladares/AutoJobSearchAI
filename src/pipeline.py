"""
Pipeline principal de AutoJobSearchAI.

Versión simplificada (sin IA):
-------------------------------
- Se eliminó la etapa de RANKING (scoring con LLM). El único filtro es
  match de título contra config/desired_titles.json (ver src/filter.py).
- Cleanup unificado: se elimina cualquier job con más de RETENTION_DAYS
  días desde que fue scrapeado (created_at), sin importar si fue
  entregado o no. Antes había dos reglas separadas (jobs entregados vs.
  rechazados) que dependían de 'score', columna que ya no existe.
- SCRAPE_KEYWORDS ya no está hardcodeado: se carga desde
  config/desired_titles.json para no mantener dos listas de "qué busco"
  desincronizadas. Scrapers con categorías fijas (ej. Chiletrabajos) lo
  ignoran; scrapers con búsqueda por texto (futuros) lo usan como query.

Notas heredadas que siguen aplicando:
- first_run_complete se guarda como clave de primer nivel en state.json,
  no dentro de 'stages' (que se resetea en cada run exitoso). Esto evita
  que el pipeline entre siempre en modo FIRST RUN.
- El parámetro `pages` que se pasa a los scrapers es solo un tope de
  seguridad; el corte real lo define la fecha en cada scraper.
- Facilitar nuevos scrapers: load_scrapers() descubre automáticamente
  cualquier módulo en src/scrapers/ que exporte run_scraper().
"""

from src.db import init_db, ensure_db_dir
from src.scrapers import load_scrapers
from src.filter import run_filter
from src.output import run_output
import sqlite3
import json
import os
import sys
from datetime import datetime, timedelta

STATE_PATH  = "config/state.json"
LOCK_PATH   = STATE_PATH + ".lock"
DB_PATH     = "data/jobs.db"
TITLES_PATH = "config/desired_titles.json"

# Tope máximo de páginas por scraper/categoría (seguridad).
# El corte real lo define la fecha en cada scraper (MAX_AGE_DAYS).
MAX_PAGES_SAFETY = 50

# Días de retención: cualquier job scrapeado hace más de esto se purga
# de la base de datos, haya sido entregado en un reporte o no.
RETENTION_DAYS = 7


def load_scrape_keywords() -> list[str]:
    """
    Carga los títulos deseados desde config/desired_titles.json para
    usarlos como query en scrapers que soporten búsqueda por texto
    (ej. GetOnBoard). Scrapers con categorías fijas (Chiletrabajos)
    reciben esta lista pero la ignoran.
    """
    if not os.path.exists(TITLES_PATH):
        print(f"[warn] {TITLES_PATH} no existe. SCRAPE_KEYWORDS quedará vacío.")
        return []
    with open(TITLES_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("desired_titles", [])


# ---------------------------------------------------------------------------
# Detección de plataforma para file locking
# ---------------------------------------------------------------------------

if sys.platform != "win32":
    import fcntl
    _FCNTL_AVAILABLE = True
else:
    _FCNTL_AVAILABLE = False


# ---------------------------------------------------------------------------
# Lock de proceso basado en PID
# ---------------------------------------------------------------------------

def acquire_pipeline_lock() -> None:
    """
    Implementa un lockfile basado en PID que sobrevive a reinicios abruptos.
    """
    if not _FCNTL_AVAILABLE:
        _write_lockfile()
        return

    if os.path.exists(LOCK_PATH):
        try:
            with open(LOCK_PATH, "r") as f:
                existing_pid = int(f.read().strip())
        except (ValueError, OSError):
            existing_pid = None

        if existing_pid and _pid_is_running(existing_pid):
            raise RuntimeError(
                f"El pipeline ya está corriendo (PID {existing_pid}). "
                "Revisa tus cron jobs o procesos activos."
            )
        else:
            print(f"[lock] Lock huérfano encontrado (PID {existing_pid} ya no existe). Tomando el lock.")

    _write_lockfile()


def _write_lockfile() -> None:
    os.makedirs(os.path.dirname(LOCK_PATH), exist_ok=True)
    with open(LOCK_PATH, "w") as f:
        f.write(str(os.getpid()))


def release_pipeline_lock() -> None:
    try:
        os.remove(LOCK_PATH)
    except FileNotFoundError:
        pass


def _pid_is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------

def load_state() -> dict:
    if not os.path.exists(STATE_PATH):
        return {"last_run": None, "first_run_complete": False, "stages": {}}
    with open(STATE_PATH, "r") as f:
        data = json.load(f)
    # Migración: versiones anteriores no tenían first_run_complete como clave
    # de primer nivel. Se infiere de stages para no perder el estado.
    if "first_run_complete" not in data:
        legacy = data.get("stages", {}).get("first_run_complete", {})
        data["first_run_complete"] = legacy.get("status") == "ok"
    if "stages" not in data:
        data["stages"] = {}
    return data


def save_state(state: dict) -> None:
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    tmp_path = STATE_PATH + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(state, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, STATE_PATH)


def mark_stage(state: dict, stage: str, status: str = "ok", error=None) -> None:
    state["stages"][stage] = {
        "status": status,
        "timestamp": datetime.utcnow().isoformat(),
        "error": error,
    }
    save_state(state)


def is_first_run(state: dict) -> bool:
    """
    Usa la clave de primer nivel 'first_run_complete', que persiste entre runs.
    Ya no depende de 'stages', que se resetea al terminar cada pipeline exitoso.
    """
    return not state.get("first_run_complete", False)


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

def run_cleanup(days: int = RETENTION_DAYS) -> None:
    """
    Elimina todos los jobs con más de `days` días desde que fueron
    scrapeados (created_at), entregados o no.

    Regla única de retención: una oferta scrapeada hace más de `days`
    días se considera expirada. Reemplaza a las dos reglas anteriores
    (jobs entregados vs. rechazados por score), que ya no tienen sentido
    sin ranking por IA.
    """
    ensure_db_dir()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    result = c.execute(
        "DELETE FROM jobs WHERE created_at < ?",
        (cutoff,)
    )
    deleted = result.rowcount
    conn.commit()
    conn.close()

    if deleted > 0:
        print(f"Cleanup: {deleted} jobs eliminados (>{days} días desde su scraping).")
    else:
        print("Cleanup: nada que eliminar.")


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def run_pipeline() -> None:
    acquire_pipeline_lock()
    try:
        _run_pipeline_inner()
    finally:
        release_pipeline_lock()


def _run_pipeline_inner() -> None:
    state = load_state()
    first_run = is_first_run(state)

    print("=== INIT ===")
    init_db()

    if first_run:
        print("Modo: FIRST RUN — el corte de antigüedad (7 días) aplica igual que en runs normales.")
        print("      El scraper avanzará hasta encontrar ofertas más antiguas o llegar al tope de seguridad.")
    else:
        print(f"Modo: DAILY RUN | Último run: {state['last_run']}")

    scrapers = load_scrapers()
    if not scrapers:
        raise RuntimeError(
            "No se encontró ningún scraper en src/scrapers/. "
            "Verifica que los archivos exporten run_scraper(pages, keywords)."
        )

    scrape_keywords = load_scrape_keywords()

    print(f"Scrapers activos: {list(scrapers.keys())}")
    print(f"Keywords (desde desired_titles.json): {scrape_keywords}")
    print(f"Tope máximo de páginas por scraper: {MAX_PAGES_SAFETY}")

    # -------------------------------------------------------------------
    # Cleanup
    # -------------------------------------------------------------------
    print("\n=== CLEANUP ===")
    try:
        run_cleanup(days=RETENTION_DAYS)
        mark_stage(state, "cleanup")
    except Exception as e:
        mark_stage(state, "cleanup", status="error", error=str(e))
        raise

    # -------------------------------------------------------------------
    # Scraping
    # -------------------------------------------------------------------
    print("\n=== SCRAPING ===")
    try:
        for name, run_scraper in scrapers.items():
            print(f"\n--- Scraper: {name} ---")
            run_scraper(pages=MAX_PAGES_SAFETY, keywords=scrape_keywords)
        mark_stage(state, "scraping")
    except Exception as e:
        mark_stage(state, "scraping", status="error", error=str(e))
        raise

    # -------------------------------------------------------------------
    # Filtering
    # -------------------------------------------------------------------
    print("\n=== FILTERING ===")
    try:
        run_filter()
        mark_stage(state, "filtering")
    except Exception as e:
        mark_stage(state, "filtering", status="error", error=str(e))
        raise

    # -------------------------------------------------------------------
    # Output
    # -------------------------------------------------------------------
    print("\n=== OUTPUT ===")
    try:
        run_output()
        mark_stage(state, "output")
    except Exception as e:
        mark_stage(state, "output", status="error", error=str(e))
        raise

    # Marcar first_run como completado en clave de primer nivel (persiste entre runs)
    if first_run:
        state["first_run_complete"] = True

    state["last_run"] = datetime.utcnow().isoformat()
    state["stages"] = {}   # se resetean los stages de este run
    save_state(state)

    print(f"\nEstado actualizado: last_run = {state['last_run']}")
    print("\n=== DONE ===")


if __name__ == "__main__":
    run_pipeline()