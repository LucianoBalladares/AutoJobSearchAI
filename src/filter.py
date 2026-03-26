"""
Módulo de filtrado por keywords para AutoJobSearchAI.

Estrategia: co-ocurrencia de categorías (Opción B)
---------------------------------------------------
Un job pasa el filtro (filtered=1) solo si cumple las tres condiciones:

1. No contiene ninguna frase negativa exacta (multi-palabra).
2. No contiene ninguna palabra negativa como palabra completa.
3. Contiene al menos una keyword de 'positive_health'
   Y al menos una keyword de 'positive_data'.

La condición 3 refleja el perfil dual del candidato: tecnólogo médico
con especialización en informática en salud y BI. Un rol puramente
clínico sin componente analítico, o un rol de datos sin contexto de
salud, no encaja con el perfil y se descarta antes de llegar al ranker.

Esto reduce el ruido significativamente respecto al OR puro anterior
sin depender de llamadas adicionales al LLM.
"""

import sqlite3
import json
import re
import unicodedata

from src.db import DB_PATH

KEYWORDS_PATH = "config/keywords.json"


def normalize(text: str) -> str:
    """
    Normaliza texto para comparación robusta:
    - Minúsculas
    - Elimina acentos / diacríticos (á→a, é→e, ñ→n, ü→u, etc.)

    Evita falsos negativos cuando una oferta escribe "Análisis"
    pero el keyword está como "analisis" (o viceversa).
    """
    text = text.lower()
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    return text


def load_keywords() -> dict:
    """
    Carga y pre-normaliza keywords desde el JSON de configuración.
    Lanza una excepción explícita si el archivo no existe o tiene
    sintaxis inválida.

    Retorna un dict con las claves:
        positive_health, positive_data, negative, negative_phrases
    """
    try:
        with open(KEYWORDS_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Archivo de keywords no encontrado: {KEYWORDS_PATH}. "
            "Asegúrate de que el archivo existe antes de correr el filtro."
        )
    except json.JSONDecodeError as e:
        raise ValueError(
            f"Error de sintaxis en {KEYWORDS_PATH}: {e}. "
            "Verifica que el JSON sea válido antes de continuar."
        )

    return {
        "positive_health":  [normalize(k) for k in raw.get("positive_health", [])],
        "positive_data":    [normalize(k) for k in raw.get("positive_data", [])],
        "negative":         [normalize(k) for k in raw.get("negative", [])],
        "negative_phrases": [normalize(k) for k in raw.get("negative_phrases", [])],
    }


def _has_match(text: str, keywords: list[str]) -> bool:
    """
    Retorna True si el texto contiene al menos una de las keywords
    como palabra completa (boundary \\b).
    Funciona para keywords de una o múltiples palabras.
    """
    for kw in keywords:
        if re.search(rf"\b{re.escape(kw)}\b", text):
            return True
    return False


def keyword_filter(
    text: str,
    positive_health: list[str],
    positive_data: list[str],
    negative: list[str],
    negative_phrases: list[str],
) -> int:
    """
    Filtro de co-ocurrencia en cuatro pasos sobre texto ya normalizado:

    1. Rechaza si contiene alguna frase negativa exacta (multi-palabra).
    2. Rechaza si contiene alguna palabra negativa como palabra completa.
    3. Rechaza si NO contiene al menos una keyword de positive_health.
    4. Rechaza si NO contiene al menos una keyword de positive_data.

    Solo retorna 1 (pasa) si supera los cuatro pasos.
    """
    t = normalize(text)

    # Paso 1: frases negativas exactas
    for phrase in negative_phrases:
        if phrase in t:
            return 0

    # Paso 2: palabras negativas con word boundary
    for word in negative:
        if re.search(rf"\b{re.escape(word)}\b", t):
            return 0

    # Paso 3 & 4: co-ocurrencia — debe tener AMBAS categorías
    has_health = _has_match(t, positive_health)
    has_data   = _has_match(t, positive_data)

    if has_health and has_data:
        return 1

    return 0


def init_column():
    """Agrega la columna 'filtered' si no existe (migración segura)."""
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

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    rows = c.execute("""
        SELECT id, title, description
        FROM jobs
        WHERE filtered IS NULL
    """).fetchall()

    accepted = rejected_negative = rejected_no_health = rejected_no_data = 0

    for job_id, title, desc in rows:
        text = f"{title} {desc or ''}"
        t = normalize(text)

        # Paso 1 & 2: negativos
        blocked = False
        for phrase in keywords["negative_phrases"]:
            if phrase in t:
                blocked = True
                break
        if not blocked:
            for word in keywords["negative"]:
                if re.search(rf"\b{re.escape(word)}\b", t):
                    blocked = True
                    break

        if blocked:
            c.execute("UPDATE jobs SET filtered=0 WHERE id=?", (job_id,))
            rejected_negative += 1
            continue

        # Paso 3 & 4: co-ocurrencia
        has_health = _has_match(t, keywords["positive_health"])
        has_data   = _has_match(t, keywords["positive_data"])

        if has_health and has_data:
            c.execute("UPDATE jobs SET filtered=1 WHERE id=?", (job_id,))
            accepted += 1
        elif not has_health:
            c.execute("UPDATE jobs SET filtered=0 WHERE id=?", (job_id,))
            rejected_no_health += 1
        else:
            c.execute("UPDATE jobs SET filtered=0 WHERE id=?", (job_id,))
            rejected_no_data += 1

    conn.commit()
    conn.close()

    print(
        f"Filtering done. "
        f"Aceptados: {accepted} | "
        f"Rechazados por negativos: {rejected_negative} | "
        f"Sin contexto salud: {rejected_no_health} | "
        f"Sin componente datos: {rejected_no_data}"
    )


if __name__ == "__main__":
    run_filter()