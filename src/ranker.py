from dotenv import load_dotenv
import sqlite3
from openai import OpenAI
import os
import re

load_dotenv()
DB_PATH = "data/jobs.db"
PROFILE_PATH = "config/profile.txt"

api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise ValueError("OPENAI_API_KEY not set")

# modelo leído desde .env en vez de hardcoded
model = os.getenv("OPENAI_MODEL")
if not model:
    raise ValueError("OPENAI_MODEL not set in .env")

client = OpenAI(api_key=api_key)


def init_column():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("PRAGMA table_info(jobs)")
    columns = [col[1] for col in c.fetchall()]

    if "score" not in columns:
        c.execute("ALTER TABLE jobs ADD COLUMN score INTEGER")

    conn.commit()
    conn.close()


def load_profile():
    with open(PROFILE_PATH, "r") as f:
        return f.read()


def score_job(description, profile):
    prompt = f"""
Evaluate this job for the following candidate:

{profile}

Job description:
{description[:2000]}

Return ONLY a number from 1 to 10.
"""

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )

        text = response.choices[0].message.content.strip()

        # parseo robusto — extrae el primer número del texto
        match = re.search(r'\d+', text)
        return int(match.group()) if match else None

    except Exception:
        return None


def run_ranker(limit=20):
    # init_column() ahora se llama automáticamente dentro de run_ranker()
    init_column()

    profile = load_profile()

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    rows = c.execute("""
        SELECT id, description 
        FROM jobs
        WHERE filtered = 1 AND score IS NULL
        LIMIT ?
    """, (limit,)).fetchall()

    for job_id, desc in rows:
        if not desc:
            continue

        print(f"Scoring job {job_id}...")

        score = score_job(desc, profile)

        if score is not None:
            c.execute(
                "UPDATE jobs SET score=? WHERE id=?",
                (score, job_id)
            )

    conn.commit()
    conn.close()
    print("Ranking done.")


if __name__ == "__main__":
    run_ranker()