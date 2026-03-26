from dotenv import load_dotenv
import sqlite3
from openai import OpenAI
import os
import re
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from src.db import DB_PATH

load_dotenv()

PROFILE_PATH = "config/profile.txt"

# Longitud máxima del perfil enviado al modelo.
# El perfil completo se recorta para no repetir tokens innecesarios
# en cada llamada cuando se rankean muchos jobs de una sola vez.
PROFILE_MAX_CHARS = 1500
DESCRIPTION_MAX_CHARS = 2000

# Modelos de OpenAI soportados. Si el valor en .env no está en esta lista
# se lanza un error claro en lugar de un fallo críptico de la API.
KNOWN_OPENAI_MODELS = {
    # Nueva generación 
    "gpt-5",
    "gpt-5-mini",
    "gpt-5-turbo",

    # Generación anterior 
    "gpt-4o",
    "gpt-4o-mini",

    # Compatibilidad legacy (pueden deprecarse)
    "gpt-4-turbo",
    "gpt-4",
    "gpt-3.5-turbo",
}

api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise ValueError("OPENAI_API_KEY not set")

model = os.getenv("OPENAI_MODEL")
if not model:
    raise ValueError("OPENAI_MODEL not set in .env")

if model not in KNOWN_OPENAI_MODELS:
    raise ValueError(
        f"OPENAI_MODEL '{model}' no reconocido. "
        f"Valores válidos: {', '.join(sorted(KNOWN_OPENAI_MODELS))}. "
        f"Si es un modelo nuevo, agrégalo a KNOWN_OPENAI_MODELS en ranker.py."
    )

client = OpenAI(api_key=api_key)


def load_profile() -> str:
    with open(PROFILE_PATH, "r") as f:
        text = f.read()
    if len(text) > PROFILE_MAX_CHARS:
        text = text[:PROFILE_MAX_CHARS] + "\n[perfil recortado]"
    return text


# Retry en errores transitorios de la API (rate limit, timeout, error 5xx).
# No reintenta en errores de autenticación (4xx que no sean 429).
@retry(
    retry=retry_if_exception_type(Exception),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
def _call_api(description: str, profile: str) -> str:
    prompt = f"""
Evaluate this job for the following candidate:

{profile}

Job description:
{description[:DESCRIPTION_MAX_CHARS]}

Return ONLY a single integer from 1 to 10. No explanation, no punctuation, just the number.
"""
    response = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    return response.choices[0].message.content.strip()


def score_job(description: str, profile: str):
    try:
        text = _call_api(description, profile)

        match = re.search(r'\b(\d{1,2})\b', text)
        if not match:
            return None

        score = int(match.group(1))

        if not (1 <= score <= 10):
            print(f"  [warn] Score fuera de rango: {score} — descartado")
            return None

        return score

    except Exception as e:
        print(f"  [error] score_job falló tras reintentos: {e}")
        return None


def run_ranker(limit=20):
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
        else:
            print(f"  [skip] Job {job_id} sin score válido")

    conn.commit()
    conn.close()
    print("Ranking done.")


if __name__ == "__main__":
    run_ranker()