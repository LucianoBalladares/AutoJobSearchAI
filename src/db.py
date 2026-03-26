"""
Módulo central de base de datos.

Fuente de verdad del schema de jobs. Todos los scrapers y módulos
deben llamar a init_db() desde aquí en lugar de definir el schema
por su cuenta. Esto evita duplicación y conflictos al agregar
nuevas columnas en el futuro.
"""

import sqlite3

DB_PATH = "data/jobs.db"


def get_connection() -> sqlite3.Connection:
    return sqlite3.connect(DB_PATH)


def init_db():
    """
    Crea la tabla jobs con todas las columnas en una sola operación.
    Incluye migración segura para bases de datos creadas con versiones anteriores.
    """
    conn = sqlite3.connect(DB_PATH)
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
    conn.commit()

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

    conn.commit()
    conn.close()