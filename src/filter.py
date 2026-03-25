import sqlite3
import json
import re
import unicodedata

DB_PATH = "data/jobs.db"
KEYWORDS_PATH = "config/keywords.json"


def normalize(text: str) -> str:
    """
    Normaliza texto para comparación robusta:
    - Minúsculas
    - Elimina acentos / diacríticos (á→a, é→e, ñ→n, ü→u, etc.)

    Esto evita falsos negativos cuando una oferta escribe "Análisis"
    pero el keyword está como "analisis" (o viceversa).
    """
    text = text.lower()
    # NFD descompone caracteres acentuados en base + diacrítico
    # category Mn = Mark, Nonspacing (los diacríticos)
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    return text


def load_keywords():
    try:
        with open(KEYWORDS_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception:
        return {"positive": [], "negative": [], "negative_phrases": []}

    # Pre-normaliza todas las listas para no repetir el trabajo en cada oferta
    return {
        "positive":          [normalize(k) for k in raw.get("positive", [])],
        "negative":          [normalize(k) for k in raw.get("negative", [])],
        "negative_phrases":  [normalize(k) for k in raw.get("negative_phrases", [])],
    }


def keyword_filter(text: str, positive: list, negative: list, negative_phrases: list) -> int:
    """
    Lógica de filtrado en tres pasos sobre texto ya normalizado:

    1. Rechaza si contiene alguna frase negativa exacta (multi-palabra).
    2. Rechaza si contiene alguna palabra negativa como palabra completa (\\bword\\b).
       — Mismo criterio que positivas: evita falsos positivos por substrings.
    3. Acepta si contiene al menos una keyword positiva como palabra completa.
       — Antes usaba `in` (substring), ahora usa \\b para consistencia.
         Ej: "pastoral" ya no matchea "sql"; "data" sí matchea "data analyst".
    """
    t = normalize(text)

    # Paso 1: frases negativas exactas (substring match es correcto aquí)
    for phrase in negative_phrases:
        if phrase in t:
            return 0

    # Paso 2: palabras negativas — word boundary para evitar falsos positivos
    for word in negative:
        if re.search(rf"\b{re.escape(word)}\b", t):
            return 0

    # Paso 3: al menos una keyword positiva — word boundary para consistencia
    for p in positive:
        if re.search(rf"\b{re.escape(p)}\b", t):
            return 1

    return 0


def init_column():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute("PRAGMA table_info(jobs)")
    columns = [col[1] for col in c.fetchall()]

    if "filtered" not in columns:
        c.execute("ALTER TABLE jobs ADD COLUMN filtered INTEGER")

    conn.commit()
    conn.close()


def run_filter():
    init_column()

    keywords = load_keywords()
    positive         = keywords["positive"]
    negative         = keywords["negative"]
    negative_phrases = keywords["negative_phrases"]

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    rows = c.execute("""
        SELECT id, title, description
        FROM jobs
        WHERE filtered IS NULL
    """).fetchall()

    accepted = rejected = 0
    for job_id, title, desc in rows:
        text = f"{title} {desc or ''}"
        result = keyword_filter(text, positive, negative, negative_phrases)
        c.execute("UPDATE jobs SET filtered=? WHERE id=?", (result, job_id))
        if result:
            accepted += 1
        else:
            rejected += 1

    conn.commit()
    conn.close()

    print(f"Filtering done. Aceptados: {accepted} | Rechazados: {rejected}")


if __name__ == "__main__":
    run_filter()