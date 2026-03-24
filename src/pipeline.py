from src.scrapers.chiletrabajos import run_scraper
from src.filter import run_filter
from src.ranker import run_ranker, init_column as init_ranker
from src.output import run_output
import sqlite3
import json
import os
from datetime import datetime, timedelta

STATE_PATH = "config/state.json"
DB_PATH = "data/jobs.db"

FIRST_RUN_PAGES = 25
DAILY_RUN_PAGES = 2


def load_state():
    if not os.path.exists(STATE_PATH):
        return {"last_run": None, "stages": {}}
    with open(STATE_PATH, "r") as f:
        data = json.load(f)
    # Compatibilidad hacia atrás: si no existe "stages", lo inicializa
    if "stages" not in data:
        data["stages"] = {}
    return data


def save_state(state):
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w") as f:
        json.dump(state, f, indent=2)


def mark_stage(state, stage, status="ok", error=None):
    """Registra el resultado de cada etapa en el estado."""
    state["stages"][stage] = {
        "status": status,
        "timestamp": datetime.utcnow().isoformat(),
        "error": error
    }
    # Guardado inmediato tras cada etapa — si el pipeline falla a mitad,
    # el estado refleja exactamente hasta dónde llegó
    save_state(state)


def is_first_run(state):
    return state.get("last_run") is None


def run_cleanup(days=7):
    """Elimina jobs entregados hace más de `days` días."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("PRAGMA table_info(jobs)")
    columns = [col[1] for col in c.fetchall()]
    if "delivered_at" not in columns:
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
        print(f"Cleanup: {deleted} jobs eliminados (delivered hace +{days} días)")
    else:
        print("Cleanup: nada que eliminar.")


def run_pipeline():
    state = load_state()
    first_run = is_first_run(state)

    print("=== INIT ===")
    if first_run:
        pages = FIRST_RUN_PAGES
        print(f"Modo: FIRST RUN — revisando {pages} páginas (~1 semana)")
    else:
        pages = DAILY_RUN_PAGES
        print(f"Modo: DAILY RUN — revisando {pages} páginas (~últimas 24h)")
        print(f"Último run completo: {state['last_run']}")

    # Muestra resumen de etapas previas si existen
    if state["stages"]:
        print("Estado etapas previas:")
        for stage, info in state["stages"].items():
            print(f"  {stage}: {info['status']} @ {info['timestamp']}")

    init_ranker()

    # Cada etapa se ejecuta de forma independiente.
    # Si falla, se registra el error y se lanza la excepción para detener el pipeline.
    # En la próxima ejecución, `state.json` mostrará exactamente qué etapa falló.

    print("\n=== CLEANUP ===")
    try:
        run_cleanup(days=7)
        mark_stage(state, "cleanup")
    except Exception as e:
        mark_stage(state, "cleanup", status="error", error=str(e))
        raise

    print("\n=== SCRAPING ===")
    try:
        run_scraper(pages=pages, keyword="data")
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

    # last_run solo se actualiza si todas las etapas completaron exitosamente
    state["last_run"] = datetime.utcnow().isoformat()
    state["stages"] = {}  # Reset etapas al completar con éxito
    save_state(state)

    print(f"\nEstado actualizado: last_run = {state['last_run']}")
    print("\n=== DONE ===")


if __name__ == "__main__":
    run_pipeline()