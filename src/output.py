import sqlite3
from datetime import datetime
import os
import json

from src.db import DB_PATH

OUTPUT_DIR = "output"
CONFIG_PATH = "config/output_config.json"


def load_min_score():
    try:
        with open(CONFIG_PATH, "r") as f:
            return json.load(f).get("min_score", 6)
    except Exception:
        return 6


def get_output_path() -> str:
    """
    Genera el path del archivo de output con fecha y hora en el nombre.
    Formato: jobs_YYYY-MM-DD_HH-MM.md
    Evita sobreescritura si el pipeline corre más de una vez al día.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    return os.path.join(OUTPUT_DIR, f"jobs_{timestamp}.md")


def fetch_jobs(min_score):
    """
    Retorna jobs con score >= min_score que aún no han sido entregados.
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute("PRAGMA table_info(jobs)")
        columns = {col[1] for col in c.fetchall()}

        if "score" not in columns:
            print("Warning: columna 'score' no existe. Ejecuta el ranker primero.")
            return []

        if "delivered_at" not in columns:
            print("Warning: columna 'delivered_at' no existe. Ejecuta init_db() primero.")
            return []

        rows = c.execute("""
            SELECT id, title, company, location, url, score, date
            FROM jobs
            WHERE filtered = 1
              AND score >= ?
              AND delivered_at IS NULL
            ORDER BY score DESC
        """, (min_score,)).fetchall()

        return list(rows)
    finally:
        conn.close()


def fetch_all_ranked_undelivered():
    """
    Retorna IDs de todos los jobs que ya fueron rankeados pero aún no
    marcados como delivered, independientemente de su score.

    Esto garantiza que jobs con score bajo no queden huérfanos en la DB
    indefinidamente: se marcan como delivered aunque no aparezcan en el output,
    lo que permite que el cleanup de 7 días los elimine correctamente.
    """
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        rows = c.execute("""
            SELECT id FROM jobs
            WHERE filtered = 1
              AND score IS NOT NULL
              AND delivered_at IS NULL
        """).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


def mark_as_delivered(job_ids):
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


def generate_markdown(jobs, min_score):
    lines = []
    today = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines.append(f"# Jobs — {today}\n")
    lines.append(f"_Mostrando {len(jobs)} ofertas con score >= {min_score}_\n")

    if not jobs:
        lines.append("_No hay jobs nuevos con score suficiente hoy._")
        return "\n".join(lines)

    for i, (job_id, title, company, location, url, score, date) in enumerate(jobs, 1):
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
    min_score = load_min_score()
    jobs = fetch_jobs(min_score)

    md = generate_markdown(jobs, min_score)

    # Marca como delivered TODOS los jobs rankeados pendientes, no solo
    # los que aparecen en el output. Los de score bajo también se marcan
    # para que el cleanup de 7 días pueda eliminarlos correctamente.
    all_ids = fetch_all_ranked_undelivered()
    mark_as_delivered(all_ids)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = get_output_path()
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(md)

    print(f"Output generado: {output_path} ({len(jobs)} jobs en reporte)")
    return output_path


if __name__ == "__main__":
    run_output()