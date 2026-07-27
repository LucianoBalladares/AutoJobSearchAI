"""
Módulo central de base de datos.

Fuente de verdad del schema de jobs. Todos los scrapers y módulos
deben llamar a init_db() desde aquí en lugar de definir el schema
por su cuenta. Esto evita duplicación y conflictos al agregar
nuevas columnas en el futuro.

Cambios (simplificación del pipeline, sin IA):
-----------------------------------------------
- Se eliminó la columna 'score' (dependía del ranking con LLM, ya no existe).
- 'filtered' se mantiene como nombre de columna pero cambia de semántica:
  ahora indica si el título del job coincide con la lista de títulos
  deseados (config/desired_titles.json), no si pasó un filtro de keywords
  por categoría.

Nota sobre get_connection()
---------------------------
Retorna un context manager que garantiza el cierre de la conexión
al salir del bloque `with`, incluso si ocurre una excepción.
sqlite3.Connection soporta `with conn:` para manejar transacciones
(commit/rollback), pero NO cierra la conexión automáticamente.
El wrapper contextmanager aquí resuelve ambas cosas a la vez:
commit/rollback + cierre garantizado.
"""

import sqlite3
from contextlib import contextmanager

DB_PATH = "data/jobs.db"


@contextmanager
def get_connection():
    """
    Context manager que abre una conexión SQLite, la cede al bloque `with`,
    hace commit si no hubo excepciones, rollback si las hubo, y siempre
    cierra la conexión al salir.

    Uso:
        with get_connection() as conn:
            c = conn.cursor()
            c.execute(...)
            conn.commit()   # opcional: get_connection ya hace commit al salir
    """
    conn = sqlite3.connect(DB_PATH)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """
    Crea la tabla jobs con todas las columnas en una sola operación.
    Incluye migración segura para bases de datos creadas con versiones
    anteriores, incluyendo la eliminación de la columna 'score' (obsoleta
    tras quitar el ranking con IA).
    """
    with get_connection() as conn:
        c = conn.cursor()
        c.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            title        TEXT,
            company      TEXT,
            location     TEXT,
            description  TEXT,
            url          TEXT UNIQUE,
            date         TEXT,
            source       TEXT,
            created_at   TEXT,
            filtered     INTEGER,
            delivered_at TEXT
        )
        """)

        c.execute("PRAGMA table_info(jobs)")
        existing = {col[1] for col in c.fetchall()}

        # Migración: añade columnas faltantes si la tabla ya existía
        # sin ellas (bases de datos creadas con versiones anteriores).
        migrations = {
            "filtered":     "ALTER TABLE jobs ADD COLUMN filtered INTEGER",
            "delivered_at": "ALTER TABLE jobs ADD COLUMN delivered_at TEXT",
        }
        for col, sql in migrations.items():
            if col not in existing:
                c.execute(sql)
                print(f"[migration] Columna '{col}' añadida.")

        # Migración de baja: elimina 'score' si la DB viene de una versión
        # anterior con ranking por IA. Requiere SQLite >= 3.35 (2021).
        # Si la versión instalada es más antigua, se deja la columna
        # huérfana sin uso — no rompe nada, solo queda sin escribirse.
        if "score" in existing:
            try:
                c.execute("ALTER TABLE jobs DROP COLUMN score")
                print("[migration] Columna 'score' eliminada (obsoleta, sin IA).")
            except sqlite3.OperationalError as e:
                print(f"[migration] No se pudo eliminar 'score' (SQLite antiguo): {e}")