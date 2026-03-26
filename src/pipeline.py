"""
Pipeline principal del sistema AutoJobSearchAI.

Cambios respecto a la versión anterior:
- El scraper ahora recibe una lista de keywords en lugar de una sola.
  Se define SCRAPE_KEYWORDS aquí como fuente de verdad para la búsqueda.
- El first run tiene protección contra repetición: se marca con una etapa
  "first_run_complete" en state.json. Si el pipeline se interrumpe antes
  de guardar last_run, el siguiente run no vuelve a descargar 25 páginas.
- init_ranker() eliminado — el schema ahora vive en src/db.py y se
  inicializa una sola vez al comienzo del pipeline.
"""

from src.db import init_db
from src.scrapers.chiletrabajos import run_scraper
from src.filter import run_filter
from src.ranker import run_ranker
from src.output import run_output
import sqlite3
import json
import os
import fcntl
from datetime import datetime, timedelta

STATE_PATH = "config/state.json"
DB_PATH = "data/jobs.db"

FIRST_RUN_PAGES = 25
DAILY_RUN_PAGES = 2

# Keywords que se usan en cada pasada de scraping.
# Agregar aquí nuevos términos de búsqueda — no hace falta tocar el scraper.
SCRAPE_KEYWORDS = [
    "data",
    "analista",
    "salud",
    "business intelligence",
    "informática",
]


# ---------------------------------------------------------------------------
# State management con file lock
# ---------------------------------------------------------------------------

def load_state() -> dict:
    if not os.path.exists(STATE_PATH):
        return {"last_run": None, "stages": {}}

    with open(STATE_PATH, "r") as f:
        fcntl.flock(f, fcntl.LOCK_SH)
        try:
            data = json.load(f)
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)

    if "stages" not in data:
        data["stages"] = {}
    return data


def save_state(state: dict):
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    tmp_path = STATE_PATH + ".tmp"

    with open(tmp_path, "w") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            json.dump(state, f, indent=2)
            f.flush()
            os.fsync(f.fileno())
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)

    os.replace(tmp_path, STATE_PATH)


def acquire_pipeline_lock():
    lock_path = STATE_PATH + ".lock"
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)
    fd = open(lock_path, "w")
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        fd.close()
        raise RuntimeError(
            "El pipeline ya está corriendo en otra instancia. "
            "Revisa tus cron jobs o procesos activos."
        )
    return fd


def mark_stage(state: dict, stage: str, status: str = "ok", error=None):
    state["stages"][stage] = {
        "status": status,
        "timestamp": datetime.utcnow().isoformat(),
        "error": error,
    }
    save_state(state)


def is_first_run(state: dict) -> bool:
    """
    Se considera first run si nunca se completó exitosamente uno anterior.
    Usa la etapa 'first_run_complete' como bandera explícita, separada de
    last_run — así, si el pipeline se interrumpe antes de guardar last_run,
    el siguiente intento no repite las 25 páginas innecesariamente.
    """
    return state["stages"].get("first_run_complete", {}).get("status") != "ok"


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

def run_cleanup(days: int = 7):
    """
    Elimina jobs entregados (delivered_at IS NOT NULL) hace más de `days` días.
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("PRAGMA table_info(jobs)")
    columns = {col[1] for col in c.fetchall()}
    if "delivered_at" not in columns:
        print("Cleanup: columna delivered_at no existe aún, saltando.")
        conn.close()
        return

    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    result = c.execute(
        "DELETE FROM jobs WHERE delivered_at IS NOT NULL AND delivered_at < ?",
        (cutoff,)
    )
    deleted = result.rowcount
    conn.commit()
    conn.close()

    if deleted > 0:
        print(f"Cleanup: {deleted} jobs entregados eliminados (>{days} días).")
    else:
        print("Cleanup: nada que eliminar.")


def run_cleanup_rejected(days: int = 30):
    """
    Limpieza opcional de jobs rechazados por el filtro o sin rankear.
    Llamar manualmente cuando la DB crezca demasiado.
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    result = c.execute(
        """DELETE FROM jobs
           WHERE (filtered = 0 OR score IS NULL)
             AND created_at < ?""",
        (cutoff,)
    )
    deleted = result.rowcount
    conn.commit()
    conn.close()
    print(f"Cleanup rejected: {deleted} jobs eliminados (>{days} días).")


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def run_pipeline():
    lock_fd = acquire_pipeline_lock()
    try:
        _run_pipeline_inner()
    finally:
        fcntl.flock(lock_fd, fcntl.LOCK_UN)
        lock_fd.close()


def _run_pipeline_inner():
    state = load_state()
    first_run = is_first_run(state)

    print("=== INIT ===")
    init_db()

    if first_run:
        pages = FIRST_RUN_PAGES
        print(f"Modo: FIRST RUN — revisando {pages} páginas por keyword (~1 semana)")
    else:
        pages = DAILY_RUN_PAGES
        print(f"Modo: DAILY RUN — revisando {pages} páginas por keyword (~últimas 24h)")
        print(f"Último run completo: {state['last_run']}")

    print(f"Keywords: {SCRAPE_KEYWORDS}")

    if state["stages"]:
        print("Estado etapas previas:")
        for stage, info in state["stages"].items():
            print(f"  {stage}: {info['status']} @ {info['timestamp']}")

    print("\n=== CLEANUP ===")
    try:
        run_cleanup(days=7)
        mark_stage(state, "cleanup")
    except Exception as e:
        mark_stage(state, "cleanup", status="error", error=str(e))
        raise

    print("\n=== SCRAPING ===")
    try:
        run_scraper(pages=pages, keywords=SCRAPE_KEYWORDS)
        mark_stage(state, "scraping")
    except Exception as e:
        mark_stage(state, "scraping", status="error", error=str(e))
        raise

    print("\n=== FILTERING ===")
    try:
        run_filter()
        mark_stage(state, "filtering")
    except Exception as e:
        mark_stage(state, "filtering", status="error", error=str(e))
        raise

    print("\n=== RANKING ===")
    try:
        run_ranker(limit=50 if first_run else 20)
        mark_stage(state, "ranking")
    except Exception as e:
        mark_stage(state, "ranking", status="error", error=str(e))
        raise

    print("\n=== OUTPUT ===")
    try:
        run_output()
        mark_stage(state, "output")
    except Exception as e:
        mark_stage(state, "output", status="error", error=str(e))
        raise

    # Marca el first run como completado ANTES de actualizar last_run.
    # Si este bloque se interrumpe entre estas dos líneas (muy improbable),
    # el peor caso es repetir el daily run, no el first run de 25 páginas.
    if first_run:
        mark_stage(state, "first_run_complete")

    state["last_run"] = datetime.utcnow().isoformat()
    state["stages"] = {}
    save_state(state)

    print(f"\nEstado actualizado: last_run = {state['last_run']}")
    print("\n=== DONE ===")


if __name__ == "__main__":
    run_pipeline()