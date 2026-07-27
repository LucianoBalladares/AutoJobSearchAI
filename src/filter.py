"""
Módulo de filtrado por título de puesto para AutoJobSearchAI.

Reemplaza al sistema anterior de keywords por categoría (salud/data,
positivas/negativas). Ahora la regla es simple y explícita, y opera
solo sobre el título del job (no sobre la descripción):

1. Si el título contiene alguno de los 'exclude_terms' (ej. "senior")
   → filtered=0, sin importar lo demás.
2. Si no, y el título contiene alguno de los 'desired_titles' (match de
   substring, no exacto — "analista de datos" matchea también
   "analista de datos junior", "analista de datos y bi", etc.)
   → filtered=1.
3. Si no matchea ningún título deseado → filtered=0.

La lista de títulos deseados y de exclusión vive en
config/desired_titles.json para poder editarla sin tocar código.
"""

import re
import unicodedata
import json

from src.db import get_connection

TITLES_PATH = "config/desired_titles.json"


def normalize(text: str) -> str:
    """
    Normaliza texto para comparación robusta:
    - Minúsculas
    - Elimina acentos / diacríticos (á→a, é→e, ñ→n, ü→u, etc.)
    """
    text = text.lower()
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    return text


def load_desired_titles() -> dict:
    """
    Carga y pre-normaliza la config de títulos deseados/excluidos.
    Lanza una excepción explícita si el archivo no existe o tiene
    sintaxis inválida.

    Retorna un dict con las claves: desired_titles, exclude_terms.
    """
    try:
        with open(TITLES_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Archivo de títulos deseados no encontrado: {TITLES_PATH}. "
            "Asegúrate de que el archivo existe antes de correr el filtro."
        )
    except json.JSONDecodeError as e:
        raise ValueError(
            f"Error de sintaxis en {TITLES_PATH}: {e}. "
            "Verifica que el JSON sea válido antes de continuar."
        )

    return {
        "desired_titles": [normalize(t) for t in raw.get("desired_titles", [])],
        "exclude_terms":  [normalize(t) for t in raw.get("exclude_terms", [])],
    }


def title_matches(title: str, desired_titles: list[str], exclude_terms: list[str]) -> bool:
    """
    True si el título coincide con algún título deseado Y no contiene
    ningún término de exclusión.

    exclude_terms usa word boundary (\\b) para no rechazar por accidente
    substrings dentro de otra palabra. desired_titles usa substring plano
    porque son frases (a veces multi-palabra) y queremos match amplio.
    """
    t = normalize(title or "")

    for term in exclude_terms:
        if re.search(rf"\b{re.escape(term)}\b", t):
            return False

    return any(wanted in t for wanted in desired_titles)


def run_filter():
    config = load_desired_titles()

    with get_connection() as conn:
        c = conn.cursor()

        rows = c.execute("""
            SELECT id, title
            FROM jobs
            WHERE filtered IS NULL
        """).fetchall()

        accepted = rejected = 0

        for job_id, title in rows:
            result = 1 if title_matches(title, config["desired_titles"], config["exclude_terms"]) else 0
            c.execute("UPDATE jobs SET filtered=? WHERE id=?", (result, job_id))

            if result == 1:
                accepted += 1
            else:
                rejected += 1

    print(f"Filtering done. Aceptados: {accepted} | Rechazados: {rejected}")


if __name__ == "__main__":
    run_filter()