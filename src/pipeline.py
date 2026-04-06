"""
Pipeline principal de AutoJobSearchAI.

Cambios respecto a la versión anterior:
- first_run_complete se guarda como clave de primer nivel en state.json,
  no dentro de 'stages' (que se resetea en cada run exitoso). Esto evita
  que el pipeline entre siempre en modo FIRST RUN.
- Se eliminó la lógica de FIRST_RUN_PAGES vs DAILY_RUN_PAGES. El corte
  real lo define la fecha en cada scraper (MAX_AGE_DAYS). El parámetro
  `pages` que se pasa a los scrapers es solo un tope de seguridad.
- Facilitar nuevos scrapers: load_scrapers() descubre automáticamente
  cualquier módulo en src/scrapers/ que exporte run_scraper().
"""

from src.db import init_db
from src.scrapers import load_scrapers
from src.filter import run_filter
from src.output import run_output
import sqlite3
import json
import os
import sys
from datetime import datetime, timedelta

STATE_PATH = "config/state.json"
LOCK_PATH  = STATE_PATH + ".lock"
DB_PATH    = "data/jobs.db"

# Tope máximo de páginas por scraper/categoría (seguridad).
# El corte real lo define la fecha en cada scraper (MAX_AGE_DAYS).
MAX_PAGES_SAFETY = 50

SCRAPE_KEYWORDS = [
    "data",
    "analista",
    "salud",
    "business intelligence",
    "informática",
]

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

def run_cleanup(days: int = 7) -> None:
    """Elimina jobs entregados hace más de `days` días."""
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


def run_cleanup_rejected(days: int = 30) -> None:
    """
    Elimina jobs rechazados por el filtro o sin score, creados hace más de `days` días.
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

    if deleted > 0:
        print(f"Cleanup rejected: {deleted} jobs eliminados (>{days} días).")
    else:
        print("Cleanup rejected: nada que eliminar.")


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
    print(f"Scrapers activos: {list(scrapers.keys())}")
    print(f"Keywords: {SCRAPE_KEYWORDS}")
    print(f"Tope máximo de páginas por scraper: {MAX_PAGES_SAFETY}")

    # -------------------------------------------------------------------
    # Cleanup
    # -------------------------------------------------------------------
    print("\n=== CLEANUP ===")
    try:
        run_cleanup(days=7)
        run_cleanup_rejected(days=30)
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
            run_scraper(pages=MAX_PAGES_SAFETY, keywords=SCRAPE_KEYWORDS)
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
    # Ranking
    # -------------------------------------------------------------------
    print("\n=== RANKING ===")
    try:
        from src.ranker import run_ranker
        run_ranker(limit=2000)
        mark_stage(state, "ranking")
    except Exception as e:
        mark_stage(state, "ranking", status="error", error=str(e))
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