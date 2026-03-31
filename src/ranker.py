from dotenv import load_dotenv
import sqlite3
import os
import re
import time
from openai import OpenAI, RateLimitError, APIConnectionError, APITimeoutError, APIStatusError

from src.db import DB_PATH

load_dotenv()

PROFILE_PATH = "config/profile.txt"

PROFILE_MAX_CHARS = 1500
DESCRIPTION_MAX_CHARS = 2000

# Delay entre llamadas a la API para evitar rate limits proactivamente.
# 0.5s = ~120 requests/min, bien por debajo del límite de los tiers comunes.
INTER_REQUEST_DELAY = 0.5


def _get_client() -> OpenAI:
    """
    Crea el cliente de OpenAI validando credenciales en tiempo de ejecución,
    no al importar el módulo. Esto evita que un import de ranker falle en
    ambientes sin .env configurado (CI, servidor nuevo, tests).
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError(
            "OPENAI_API_KEY no está configurada. "
            "Crea un archivo .env con OPENAI_API_KEY=sk-... antes de correr el ranker."
        )
    return OpenAI(api_key=api_key)


def _get_model() -> str:
    """
    Lee y valida el modelo en tiempo de ejecución.
    Emite un warning para modelos desconocidos en lugar de raise, para no
    bloquear al agregar modelos nuevos (ej. al migrar a Anthropic).
    """
    model = os.getenv("OPENAI_MODEL")
    if not model:
        raise ValueError(
            "OPENAI_MODEL no está configurada. "
            "Agrega OPENAI_MODEL=gpt-4o-mini (u otro) a tu archivo .env."
        )

    known_models = {
        # Nueva generación
        "gpt-5.4",
        "gpt-5.4-mini",
        "gpt-5.4-pro",
        "gpt-5.4-nano",

        # Generación anterior
        "gpt-4o",
        "gpt-4o-mini",

        # Compatibilidad legacy (pueden deprecarse)
        "gpt-4-turbo",
        "gpt-4",
        "gpt-3.5-turbo",
    }
    if model not in known_models:
        print(
            f"  [warn] OPENAI_MODEL='{model}' no está en la lista conocida. "
            f"Si es un modelo nuevo o de Anthropic, verifica que el endpoint sea compatible."
        )

    return model


def load_profile() -> str:
    with open(PROFILE_PATH, "r") as f:
        text = f.read()
    if len(text) > PROFILE_MAX_CHARS:
        text = text[:PROFILE_MAX_CHARS] + "\n[perfil recortado]"
    return text


# Excepciones transitorias de la API de OpenAI que justifican un reintento.
_RETRYABLE = (RateLimitError, APIConnectionError, APITimeoutError)

# Prompt del sistema: define el rol y el rubric de scoring una sola vez.
# Se envía como system message para separarlo del contenido variable (job + perfil).
SYSTEM_PROMPT = """\
You are a job-fit evaluator. Your task is to score how well a job offer matches
a candidate's profile on a scale from 1 to 10.

Use this rubric strictly and consistently:

9–10  Perfect fit: healthcare AND data/BI context, matches candidate's tools
      (Power BI, SQL, Python), junior or mid-level seniority.
      Example: "Analista de Datos en hospital", "Health Informatics Specialist".

7–8   Strong fit: one dimension is strong (healthcare OR data), partial tool
      overlap, or seniority slightly above but reachable.
      Example: "Data Analyst" at a health insurance company, "BI Developer" at
      a pharma firm, "Informático de Salud" with some analytics.

5–6   Partial fit: adjacent role with some overlap but missing a key dimension.
      Example: pure data role with no health context, or clinical informatics
      with no BI/analytics component.

3–4   Weak fit: role is too far from the profile but not completely unrelated.
      Example: general IT support at a hospital, software QA with no data work.

1–2   No fit: wrong sector, wrong level, or no overlap with the profile.

Penalize heavily (score ≤ 3) when:
- The role is purely sales, marketing, or customer service.
- The role requires 5+ years of senior experience.
- The role is purely clinical with zero data/analytics component.
- The role involves manual/physical labor unrelated to informatics.

Return ONLY a single integer from 1 to 10. No explanation, no punctuation, just the number.\
"""


def _build_user_prompt(description: str, profile: str) -> str:
    """
    Construye el mensaje de usuario con el perfil y la descripción del job.
    Separar system/user permite que el modelo cachee el system prompt entre
    llamadas (feature de algunos proveedores) y mantiene el contexto más limpio.
    """
    return f"""\
Candidate profile:
{profile}

Job description:
{description[:DESCRIPTION_MAX_CHARS]}

Score:\
"""


def _call_api_with_retry(
    client: OpenAI,
    model: str,
    description: str,
    profile: str,
    max_attempts: int = 3,
) -> str:
    """
    Llama a la API con retry manual sobre errores transitorios conocidos.
    Backoff exponencial: 2s → 4s → 8s.
    Errores no transitorios (4xx que no sean 429) se propagan inmediatamente.

    Usa system + user messages para un scoring más consistente:
    - system: rubric fijo, define el comportamiento del evaluador.
    - user: contenido variable (perfil + descripción del job).
    """
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": _build_user_prompt(description, profile)},
                ],
                temperature=0,
                max_completion_tokens=5,  # Solo necesitamos un entero de 1-2 dígitos
            )
            return response.choices[0].message.content.strip()

        except _RETRYABLE as e:
            last_exc = e
            wait = 2 ** attempt
            print(f"  [retry {attempt}/{max_attempts}] Error transitorio: {e}. Esperando {wait}s...")
            time.sleep(wait)

        except APIStatusError as e:
            if e.status_code >= 500:
                last_exc = e
                wait = 2 ** attempt
                print(f"  [retry {attempt}/{max_attempts}] Error 5xx ({e.status_code}). Esperando {wait}s...")
                time.sleep(wait)
            else:
                raise

    raise last_exc


def score_job(client: OpenAI, model: str, description: str, profile: str):
    """
    Devuelve un score entero 1–10 o None si la API falla o retorna algo inválido.
    Recibe client y model como parámetros para evitar recrearlos en cada llamada.
    """
    try:
        text = _call_api_with_retry(client, model, description, profile)

        match = re.search(r'\b(\d{1,2})\b', text)
        if not match:
            print(f"  [warn] Respuesta sin número válido: '{text}'")
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
    # Validar credenciales y crear recursos una sola vez para todo el batch.
    client = _get_client()
    model = _get_model()
    profile = load_profile()

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    rows = c.execute("""
        SELECT id, description
        FROM jobs
        WHERE filtered = 1 AND score IS NULL
        LIMIT ?
    """, (limit,)).fetchall()

    print(f"Jobs a rankear: {len(rows)}")

    for i, (job_id, desc) in enumerate(rows, 1):
        if not desc:
            print(f"  [skip] Job {job_id} sin descripción")
            continue

        print(f"Scoring job {job_id} ({i}/{len(rows)})...")

        score = score_job(client, model, desc, profile)

        if score is not None:
            c.execute("UPDATE jobs SET score=? WHERE id=?", (score, job_id))
            conn.commit()
            print(f"  → score: {score}")
        else:
            print(f"  [skip] Job {job_id} sin score válido")

        if i < len(rows):
            time.sleep(INTER_REQUEST_DELAY)

    conn.close()
    print("Ranking done.")


if __name__ == "__main__":
    run_ranker()