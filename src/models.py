"""
Modelos de datos compartidos para AutoJobSearchAI.

JobDict es el contrato de interfaz entre los scrapers y la base de datos.
Todos los scrapers deben construir sus dicts usando esta clase para que
el type checker detecte campos faltantes o con nombre incorrecto en tiempo
de desarrollo, no en producción.

Uso en un scraper:
    from src.models import JobDict

    jobs.append(JobDict(
        title="Analista de Datos",
        company="Clínica Las Condes",
        location="Santiago",
        description="...",
        url="https://...",
        date="01/04/2026",
        source="mi_scraper",
        created_at=datetime.utcnow().isoformat(),
    ))
"""

from typing import TypedDict


class JobDict(TypedDict):
    """
    Representa una oferta laboral tal como la produce un scraper.

    Campos obligatorios (requeridos por save_job() y el schema de la DB):
        title       — Título del cargo.
        company     — Nombre de la empresa. Cadena vacía si no está disponible.
        location    — Ciudad o región. Cadena vacía si no está disponible.
        description — Texto completo de la oferta. Cadena vacía si no se pudo obtener.
        url         — URL única de la oferta. Usado como PRIMARY KEY (UNIQUE) en la DB.
        date        — Fecha de publicación tal como la muestra el sitio (texto libre).
        source      — Identificador del scraper (ej. "chiletrabajos", "getonboard").
        created_at  — Timestamp UTC ISO 8601 del momento en que el scraper procesó la oferta.
                      Usar datetime.utcnow().isoformat().
    """
    title:       str
    company:     str
    location:    str
    description: str
    url:         str
    date:        str
    source:      str
    created_at:  str