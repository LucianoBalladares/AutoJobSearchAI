import sqlite3
from datetime import datetime
import os

DB_PATH = "data/jobs.db"
OUTPUT_PATH = "output/jobs_today.md"


def fetch_jobs(limit=10):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    rows = c.execute("""
        SELECT title, company, location, url, score
        FROM jobs
        WHERE filtered = 1 AND score IS NOT NULL
        ORDER BY score DESC
        LIMIT ?
    """, (limit,)).fetchall()

    conn.close()
    return rows


def generate_markdown(jobs):
    lines = []

    today = datetime.now().strftime("%Y-%m-%d")
    lines.append(f"# Jobs - {today}\n")

    for i, (title, company, location, url, score) in enumerate(jobs, 1):
        lines.append(f"## {i}. {title}")
        lines.append(f"- Company: {company}")
        lines.append(f"- Location: {location}")
        lines.append(f"- Score: {score}")
        lines.append(f"- Link: {url}")
        lines.append("")

    return "\n".join(lines)


def save_output(content):
    os.makedirs("output", exist_ok=True)

    with open(OUTPUT_PATH, "w") as f:
        f.write(content)


def run_output(limit=10):
    jobs = fetch_jobs(limit)
    md = generate_markdown(jobs)
    save_output(md)

    print(f"Generated {OUTPUT_PATH}")


if __name__ == "__main__":
    run_output()