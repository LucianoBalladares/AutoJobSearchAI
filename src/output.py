"""
Genera el reporte diario de ofertas laborales.

Sin ranking por IA: se incluyen todas las ofertas con filtered=1
(título coincide con desired_titles.json) que aún no fueron entregadas.
No hay score ni umbral de calidad — el filtro de título ya es la única
capa de decisión.
"""

import sqlite3
from datetime import datetime
import os

from src.db import DB_PATH, ensure_db_dir

OUTPUT_DIR = "output"


def get_output_path() -> str:
    """
    Genera el path del archivo de output con fecha y hora (incluyendo segundos)
    en el nombre. Formato: jobs_YYYY-MM-DD_HH-MM-SS.md

    Los segundos evitan colisión si el pipeline corre más de una vez
    dentro del mismo minuto (retry manual, bug en el lock, etc.).
    """
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return os.path.join(OUTPUT_DIR, f"jobs_{timestamp}.md")


def fetch_jobs():
    """
    Retorna jobs con título coincidente (filtered=1) que aún no han sido
    entregados en un reporte anterior.
    """
    ensure_db_dir()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    try:
        c.execute("PRAGMA table_info(jobs)")
        columns = {col[1] for col in c.fetchall()}

        if "delivered_at" not in columns:
            print("Warning: columna 'delivered_at' no existe. Ejecuta init_db() primero.")
            return []

        rows = c.execute("""
            SELECT id, title, company, location, url, date
            FROM jobs
            WHERE filtered = 1
              AND delivered_at IS NULL
            ORDER BY id DESC
        """).fetchall()

        return list(rows)
    finally:
        conn.close()


def mark_as_delivered(job_ids):
    if not job_ids:
        return
    ensure_db_dir()
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
    today = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines.append(f"# Jobs — {today}\n")
    lines.append(f"_Mostrando {len(jobs)} ofertas nuevas que coinciden con tus títulos deseados_\n")

    if not jobs:
        lines.append("_No hay jobs nuevos hoy._")
        return "\n".join(lines)

    for i, (job_id, title, company, location, url, date) in enumerate(jobs, 1):
        lines.append(f"## {i}. {title}")
        lines.append(f"- **Empresa:** {company or 'No especificada'}")
        lines.append(f"- **Ubicación:** {location or 'No especificada'}")
        lines.append(f"- **Publicado:** {date or 'Sin fecha'}")
        lines.append(f"- **Link:** {url}")
        lines.append("")

    return "\n".join(lines)


def run_output():
    jobs = fetch_jobs()
    md = generate_markdown(jobs)

    job_ids = [row[0] for row in jobs]
    mark_as_delivered(job_ids)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    output_path = get_output_path()
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(md)

    print(f"Output generado: {output_path} ({len(jobs)} jobs en reporte)")
    return output_path


if __name__ == "__main__":
    run_output()