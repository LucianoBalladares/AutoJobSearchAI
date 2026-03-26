"""
Módulo central de base de datos.

Fuente de verdad del schema de jobs. Todos los scrapers y módulos
deben llamar a init_db() desde aquí en lugar de definir el schema
por su cuenta. Esto evita duplicación y conflictos al agregar
nuevas columnas en el futuro.

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
    Incluye migración segura para bases de datos creadas con versiones anteriores.
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
            score        INTEGER,
            delivered_at TEXT
        )
        """)

        # Migración segura: añade columnas faltantes si la tabla ya existía
        # sin las columnas nuevas (bases de datos creadas con versiones anteriores).
        c.execute("PRAGMA table_info(jobs)")
        existing = {col[1] for col in c.fetchall()}
        migrations = {
            "filtered":     "ALTER TABLE jobs ADD COLUMN filtered INTEGER",
            "score":        "ALTER TABLE jobs ADD COLUMN score INTEGER",
            "delivered_at": "ALTER TABLE jobs ADD COLUMN delivered_at TEXT",
        }
        for col, sql in migrations.items():
            if col not in existing:
                c.execute(sql)
                print(f"[migration] Columna '{col}' añadida.")