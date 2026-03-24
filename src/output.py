import sqlite3
from datetime import datetime
import os

DB_PATH = "data/jobs.db"
OUTPUT_DIR = "output"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "jobs_today.md")

MIN_SCORE = 6  # solo jobs con score estrictamente mayor que este valor


def init_column():
    """Agrega la columna delivered_at si no existe."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("PRAGMA table_info(jobs)")
    columns = [col[1] for col in c.fetchall()]

    if "delivered_at" not in columns:
        c.execute("ALTER TABLE jobs ADD COLUMN delivered_at TEXT")
        conn.commit()
        print("Columna delivered_at creada.")

    conn.close()


def fetch_jobs():
    """
    Trae todos los jobs con score > MIN_SCORE que aún no han sido entregados,
    ordenados por score descendente. Sin LIMIT — queremos todos los relevantes.
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Verificar columnas necesarias
    c.execute("PRAGMA table_info(jobs)")
    columns = [col[1] for col in c.fetchall()]

    if "score" not in columns:
        print("Warning: columna 'score' no existe aún. Corre el ranker primero.")
        conn.close()
        return [], conn

    if "delivered_at" not in columns:
        c.execute("ALTER TABLE jobs ADD COLUMN delivered_at TEXT")
        conn.commit()

    rows = c.execute("""
        SELECT id, title, company, location, url, score, date
        FROM jobs
        WHERE filtered = 1
          AND score > ?
          AND delivered_at IS NULL
        ORDER BY score DESC
    """, (MIN_SCORE,)).fetchall()

    conn.close()
    return rows


def mark_as_delivered(job_ids):
    """Marca los jobs entregados para no repetirlos en futuros outputs."""
    if not job_ids:
        return

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    now = datetime.utcnow().isoformat()
    placeholders = ",".join("?" * len(job_ids))
    c.execute(
        f"UPDATE jobs SET delivered_at = ? WHERE id IN ({placeholders})",
        [now] + list(job_ids)
    )

    conn.commit()
    conn.close()
    print(f"{len(job_ids)} jobs marcados como delivered.")


def generate_markdown(jobs):
    lines = []
    today = datetime.now().strftime("%Y-%m-%d")
    lines.append(f"# Jobs — {today}\n")
    lines.append(f"_Mostrando {len(jobs)} ofertas con score > {MIN_SCORE}_\n")

    if not jobs:
        lines.append("_No hay jobs nuevos con score suficiente hoy._")
        return "\n".join(lines)

    for i, (job_id, title, company, location, url, score, date) in enumerate(jobs, 1):
        # Barra visual del score (ej: score 8 → ████████░░)
        filled = "█" * score
        empty = "░" * (10 - score)
        score_bar = f"{filled}{empty} {score}/10"

        lines.append(f"## {i}. {title}")
        lines.append(f"- **Empresa:** {company}")
        lines.append(f"- **Ubicación:** {location or 'No especificada'}")
        lines.append(f"- **Publicado:** {date or 'Sin fecha'}")
        lines.append(f"- **Score:** {score_bar}")
        lines.append(f"- **Link:** {url}")
        lines.append("")

    return "\n".join(lines)


def run_output():
    init_column()

    jobs = fetch_jobs()

    if not jobs:
        print("No hay jobs nuevos con score suficiente.")
        # Generar archivo igualmente para dejar registro del día
        md = generate_markdown([])
    else:
        md = generate_markdown(jobs)
        # Marcar como entregados DESPUÉS de generar el markdown
        job_ids = [row[0] for row in jobs]
        mark_as_delivered(job_ids)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(md)

    print(f"Output generado: {OUTPUT_PATH} ({len(jobs)} jobs)")


if __name__ == "__main__":
    run_output()