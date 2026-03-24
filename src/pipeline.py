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

# Cuántas páginas revisar según el modo
FIRST_RUN_PAGES = 25   # ~1 semana de ofertas
DAILY_RUN_PAGES = 2    # ~últimas 24–48h


def load_state():
    if not os.path.exists(STATE_PATH):
        return {"last_run": None}
    with open(STATE_PATH, "r") as f:
        return json.load(f)


def save_state(state):
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    with open(STATE_PATH, "w") as f:
        json.dump(state, f, indent=2)


def is_first_run(state):
    """Retorna True si nunca se ha corrido o si el last_run es None."""
    return state.get("last_run") is None


def run_cleanup(days=7):
    """
    Elimina jobs que ya fueron entregados hace más de `days` días.
    Mantiene todos los jobs sin delivered_at (pendientes) intactos
    para que el deduplication por URL siga funcionando.
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Verificar que la columna existe antes de intentar limpiar
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
    try:
        print("=== INIT ===")
        state = load_state()
        first_run = is_first_run(state)

        if first_run:
            pages = FIRST_RUN_PAGES
            print(f"Modo: FIRST RUN — revisando {pages} páginas (~1 semana)")
        else:
            pages = DAILY_RUN_PAGES
            print(f"Modo: DAILY RUN — revisando {pages} páginas (~últimas 24h)")
            print(f"Último run: {state['last_run']}")

        init_ranker()

        print("\n=== CLEANUP ===")
        run_cleanup(days=7)

        print("\n=== SCRAPING ===")
        run_scraper(pages=pages, keyword="data")

        print("\n=== FILTERING ===")
        run_filter()

        print("\n=== RANKING ===")
        run_ranker(limit=50 if first_run else 20)

        print("\n=== OUTPUT ===")
        run_output()

        # Guardar estado solo si todo salió bien
        state["last_run"] = datetime.utcnow().isoformat()
        save_state(state)
        print(f"\nEstado actualizado: last_run = {state['last_run']}")

        print("\n=== DONE ===")

    except Exception as e:
        print(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    run_pipeline()