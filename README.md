# AutoJobSearchAI

Sistema de búsqueda automatizada de empleo. Scrapea ofertas laborales,
filtra por título de puesto deseado, elimina duplicados y genera un
reporte diario en Markdown.

Sin IA: el filtrado es 100% determinístico, por texto de título.
La optimización de CV se hace manualmente, fuera de este sistema.

## Cómo funciona

1. **Scraping**: recolecta ofertas desde las fuentes activas en
   `src/scrapers/` (actualmente: Chiletrabajos).
2. **Dedup**: la columna `url` es `UNIQUE` en la base de datos — cada
   oferta se guarda una sola vez, sin importar cuántas veces se
   re-scrapee.
3. **Filtrado por título**: compara el título de cada oferta contra
   `config/desired_titles.json` (ver más abajo).
4. **Retención de 7 días**: cualquier oferta scrapeada hace más de 7
   días se elimina automáticamente de la base de datos.
5. **Output**: genera `output/jobs_<fecha>.md` con las ofertas nuevas
   que coincidieron con tus títulos deseados.

## Configurar títulos deseados

Editar `config/desired_titles.json`:

```json
{
  "desired_titles": ["analista de datos", "data analyst"],
  "exclude_terms": ["senior"]
}
```

- `desired_titles`: si el título de la oferta contiene cualquiera de
  estos textos (sin acentos, sin importar mayúsculas), se acepta. Es
  match por substring, no exacto — "analista de datos" también
  matchea "analista de datos junior" o "analista de datos y bi".
- `exclude_terms`: si el título contiene cualquiera de estos términos,
  se rechaza sin importar lo demás.

## Correr el pipeline

```
python3 -m src.pipeline
```

## Estructura

```
/src
  db.py         — schema y conexión SQLite
  filter.py     — match de título vs. desired_titles.json
  output.py     — genera el reporte Markdown
  pipeline.py   — orquesta cleanup → scraping → filtrado → output
  models.py     — contrato JobDict para scrapers
  /scrapers
    chiletrabajos.py

/config
  desired_titles.json  — qué títulos buscar / excluir
  state.json            — estado del pipeline (autogenerado)

/data
  jobs.db       — base de datos SQLite (gitignored)

/output
  jobs_*.md     — reportes generados (gitignored)
```

## Agregar un scraper nuevo

Ver `src/scrapers/__init__.py` — cualquier módulo en `src/scrapers/`
que exporte `run_scraper(pages, keywords)` se detecta automáticamente,
sin tocar `pipeline.py`.
