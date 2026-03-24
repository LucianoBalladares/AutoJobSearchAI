import sqlite3
import json

DB_PATH = "data/jobs.db"
KEYWORDS_PATH = "config/keywords.json"


def load_keywords():
    try:
        with open(KEYWORDS_PATH, "r") as f:
            return json.load(f)
    except Exception:
        return {"positive": [], "negative": [], "negative_phrases": []}


def init_column():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("PRAGMA table_info(jobs)")
    columns = [col[1] for col in c.fetchall()]

    if "filtered" not in columns:
        c.execute("ALTER TABLE jobs ADD COLUMN filtered INTEGER")

    conn.commit()
    conn.close()


def keyword_filter(text, positive, negative, negative_phrases):
    """
    Lógica de filtrado en tres pasos:
    1. Rechaza si contiene alguna frase negativa exacta (multi-palabra).
    2. Rechaza si contiene alguna palabra negativa como palabra completa (\bword\b).
    3. Acepta si contiene al menos una keyword positiva.
    """
    import re
    text_lower = text.lower()

    # Paso 1: frases negativas exactas (ej: "jefe de ventas", "call center")
    for phrase in negative_phrases:
        if phrase.lower() in text_lower:
            return 0

    # Paso 2: palabras negativas como palabra completa para evitar falsos positivos
    # "manager" no bloquea "data manager" — solo si aparece como cargo aislado
    # Esto se controla moviendo términos ambiguos a negative_phrases en keywords.json
    for word in negative:
        if re.search(rf'\b{re.escape(word)}\b', text_lower):
            return 0

    # Paso 3: al menos una keyword positiva
    for p in positive:
        if p.lower() in text_lower:
            return 1

    return 0


def run_filter():
    init_column()

    keywords = load_keywords()
    positive = keywords.get("positive", [])
    negative = keywords.get("negative", [])
    # negative_phrases: lista separada para coincidencias de frase exacta multi-palabra
    negative_phrases = keywords.get("negative_phrases", [])

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    rows = c.execute("""
        SELECT id, title, description 
        FROM jobs 
        WHERE filtered IS NULL
    """).fetchall()

    for job_id, title, desc in rows:
        text = f"{title} {desc or ''}"
        result = keyword_filter(text, positive, negative, negative_phrases)

        c.execute(
            "UPDATE jobs SET filtered=? WHERE id=?",
            (result, job_id)
        )

    conn.commit()
    conn.close()

    print("Filtering done.")


if __name__ == "__main__":
    run_filter()