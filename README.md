# AutoJobSearchAI

Sistema de búsqueda automatizada de empleo. Scrapea ofertas laborales,
filtra por título de puesto deseado, elimina duplicados y genera un
reporte diario en Markdown.

Sin IA: el filtrado es 100% determinístico, por texto de título.
La optimización de CV se hace manualmente, fuera de este sistema.

## Cómo funciona

1. **Scraping**: recolecta ofertas desde las fuentes activas en
   `src/scrapers/` — actualmente Chiletrabajos, Computrabajo Chile y
   GetOnBoard (ver detalle de cada una más abajo).
2. **Dedup**: la columna `url` es `UNIQUE` en la base de datos — cada
   oferta se guarda una sola vez, sin importar cuántas veces se
   re-scrapee ni desde cuál fuente.
3. **Filtrado por título**: compara el título de cada oferta contra
   `config/desired_titles.json` (ver más abajo).
4. **Retención de 7 días**: cualquier oferta scrapeada hace más de 7
   días se elimina automáticamente de la base de datos.
5. **Output**: genera `output/jobs_<fecha>.md` con las ofertas nuevas
   que coincidieron con tus títulos deseados.

## Fuentes activas

| Fuente             | Estrategia                                                     | Confianza                                                                                |
| ------------------ | -------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| Chiletrabajos      | Categorías fijas, HTML                                         | Alta — probado contra el sitio real                                                      |
| Computrabajo Chile | Búsqueda por título (usa `desired_titles`), HTML               | Alta — selectores confirmados contra código real                                         |
| GetOnBoard         | Búsqueda por título vía su API pública oficial (JSON, no HTML) | Media-alta — la API está confirmada, algunos nombres de campo aún no verificados en vivo |

**Descartadas deliberadamente: LinkedIn e Indeed.** Ambas requieren
sesión autenticada para ver resultados completos, lo que implica usar
tu cuenta personal para scraping automatizado — viola sus ToS y arriesga
un baneo de cuenta. Alternativa considerada pero no construida: un
parser de las alertas de empleo que ambos sitios envían por correo
(IMAP), que no toca sus sitios en absoluto.

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
- `exclude_terms`: si el título contiene cualquiera de estos términos
  como palabra completa, se rechaza sin importar lo demás — la
  exclusión gana siempre, incluso sobre un match válido de
  `desired_titles`. Ojo con términos genéricos: excluir "ingeniero"
  también excluye "Ingeniero de Datos" si ese término está en tu
  lista de deseados.

Computrabajo y GetOnBoard usan esta misma lista como término de
búsqueda (`keywords`). Chiletrabajos la ignora — usa categorías fijas.

## Correr el pipeline

```
python3 -m src.pipeline
```

Para probar un scraper individual sin correr todo el pipeline (útil
para diagnosticar, especialmente con GetOnBoard):

```
python3 -m src.scrapers.getonbrd
python3 -m src.scrapers.computrabajo
python3 -m src.scrapers.chiletrabajos
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
    __init__.py     — autodiscovery
    _common.py      — save_job, get_existing_urls, headers (compartido)
    chiletrabajos.py
    computrabajo.py
    getonbrd.py

/config
  desired_titles.json  — qué títulos buscar / excluir
  state.json            — estado del pipeline (autogenerado)

/data
  jobs.db       — base de datos SQLite (gitignored, se crea sola)

/output
  jobs_*.md     — reportes generados (gitignored)
```

## Agregar un scraper nuevo

Ver `src/scrapers/__init__.py` — cualquier módulo en `src/scrapers/`
que exporte `run_scraper(pages, keywords)` se detecta automáticamente,
sin tocar `pipeline.py`. Módulos que empiezan con `_` (como `_common.py`)
se ignoran — úsalos para helpers compartidos entre scrapers.
