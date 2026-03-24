import sqlite3
import json

DB_PATH = "data/jobs.db"
KEYWORDS_PATH = "config/keywords.json"


def load_keywords():
    try:
        with open(KEYWORDS_PATH, "r") as f:
            return json.load(f)
    except Exception:
        return {"positive": [], "negative": []}


def init_column():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("PRAGMA table_info(jobs)")
    columns = [col[1] for col in c.fetchall()]

    if "filtered" not in columns:
        c.execute("ALTER TABLE jobs ADD COLUMN filtered INTEGER")

    conn.commit()
    conn.close()


def keyword_filter(text, positive, negative):
    text = text.lower()

    if any(n in text for n in negative):
        return 0

    if any(p in text for p in positive):
        return 1

    return 0


def run_filter():
    # init_column() ahora se llama automáticamente dentro de run_filter()
    init_column()

    keywords = load_keywords()
    positive = keywords["positive"]
    negative = keywords["negative"]

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # solo procesa jobs que nunca han sido evaluados (filtered IS NULL)
    rows = c.execute("""
        SELECT id, title, description 
        FROM jobs 
        WHERE filtered IS NULL
    """).fetchall()

    for job_id, title, desc in rows:
        text = f"{title} {desc or ''}"
        result = keyword_filter(text, positive, negative)

        c.execute(
            "UPDATE jobs SET filtered=? WHERE id=?",
            (result, job_id)
        )

    conn.commit()
    conn.close()

    print("Filtering done.")


if __name__ == "__main__":
    run_filter()