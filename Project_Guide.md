# AutoJobSearchAI — Guía del Proyecto

## 1. Objetivo

Sistema automatizado que:

- Recolecta ofertas laborales desde múltiples fuentes.
- Elimina duplicados (URL única).
- Filtra por título de puesto deseado (match de texto, sin IA).
- Purga automáticamente ofertas con más de 7 días de antigüedad.
- Genera un reporte diario en Markdown con los links.

La optimización de CV y la postulación siguen siendo manuales, fuera
de este sistema.

## 2. Por qué se simplificó

La versión anterior incluía un ranking con LLM (OpenAI) que evaluaba
el fit de cada oferta en una escala 1–10, y una etapa de optimización
de CV asistida por IA. Ambas se descartaron:

- El ranking dependía de la descripción completa de cada oferta, que
  el scraper obtenía visitando la página de detalle de cada job. Esa
  request adicional fallaba con frecuencia (parseo frágil, HTML
  variable, posible rate-limiting), dejando al LLM sin texto que
  evaluar la mayoría de las veces — lo que hacía el ranking inútil en
  la práctica.
- La optimización de CV con IA no producía resultados usables; se
  terminaba haciendo manualmente de todas formas.

La solución no fue arreglar esas piezas, sino eliminarlas: el filtrado
ahora es determinístico (título vs. lista de títulos deseados) y no
depende de la descripción completa del job ni de ninguna API externa.

## 3. Arquitectura

```
Scraper (título, empresa, ubicación, extracto, link)
        ↓
DB SQLite (url UNIQUE → dedup automático)
        ↓
Filter (¿el título matchea desired_titles.json?)
        ↓
Output (jobs_YYYY-MM-DD.md)
        ↓
Cleanup (purga jobs con created_at > 7 días)
```

## 4. Componentes

### 4.1 Scrapers (`src/scrapers/`)

Autodiscovery: cualquier módulo que exporte
`run_scraper(pages: int, keywords: list[str])` se carga automáticamente.
No hace falta tocar `pipeline.py` para agregar uno nuevo.

Fuente activa actualmente:

- **Chiletrabajos** — categorías fijas, no usa `keywords`. Ya NO
  visita la página de detalle de cada oferta; usa solo lo disponible
  en el listado (título, empresa, ubicación, extracto).

Fuentes pendientes de evaluar (a discutir antes de construir):

- GetOnBoard — sin protección anti-bot agresiva conocida, buen fit
  con roles data/tech. Riesgo técnico bajo.
- Empleos Públicos — sitio de gobierno, generalmente sin protección
  agresiva. Riesgo técnico bajo.
- Indeed — protección Cloudflare fuerte, riesgo de bloqueo.
- LinkedIn — requiere sesión autenticada, viola sus ToS, alto
  mantenimiento y riesgo de bloqueo de cuenta.

### 4.2 Base de datos (`src/db.py`)

Tabla `jobs`:

| columna      | tipo        | uso                                                    |
| ------------ | ----------- | ------------------------------------------------------ |
| id           | INTEGER PK  | —                                                      |
| title        | TEXT        | título de la oferta                                    |
| company      | TEXT        | empresa                                                |
| location     | TEXT        | ubicación                                              |
| description  | TEXT        | extracto del listado (no descripción completa)         |
| url          | TEXT UNIQUE | dedup                                                  |
| date         | TEXT        | fecha tal como la muestra el sitio                     |
| source       | TEXT        | nombre del scraper                                     |
| created_at   | TEXT        | timestamp de scraping — base de la retención de 7 días |
| filtered     | INTEGER     | 1 = título coincide con desired_titles.json            |
| delivered_at | TEXT        | timestamp de cuándo se incluyó en un reporte           |

La columna `score` (del ranking con IA) fue eliminada. `init_db()`
migra automáticamente bases de datos antiguas que aún la tengan.

### 4.3 Filtro (`src/filter.py`)

Compara el título normalizado (sin acentos, minúsculas) contra
`config/desired_titles.json`:

1. Si contiene algún `exclude_terms` → rechazado.
2. Si no, y contiene algún `desired_titles` (substring) → aceptado.
3. Si no matchea nada → rechazado.

Opera solo sobre el título, no sobre la descripción.

### 4.4 Output (`src/output.py`)

Genera `output/jobs_<fecha>.md` con todas las ofertas `filtered=1` y
`delivered_at IS NULL`. Al generarse el reporte, esas ofertas se
marcan como entregadas para no repetirse en el próximo run.

### 4.5 Cleanup (`src/pipeline.py`)

Una sola regla: se elimina cualquier oferta con `created_at` de hace
más de 7 días, haya sido entregada o no.

## 5. Flujo diario

1. Correr `python3 -m src.pipeline` (manual o vía cron).
2. Abrir el `jobs_<fecha>.md` generado en `/output`.
3. Revisar los links.
4. Ajustar CV manualmente por oferta.
5. Postular.

## 6. Estructura del proyecto

```
/src
  db.py
  filter.py
  output.py
  pipeline.py
  models.py
  /scrapers
    __init__.py
    chiletrabajos.py

/config
  desired_titles.json
  state.json          (autogenerado)

/data
  jobs.db             (gitignored)

/output
  jobs_*.md           (gitignored)
```

## 7. Stack

- Python
- SQLite
- Requests + BeautifulSoup
- tenacity (reintentos HTTP)

Sin dependencia de OpenAI ni de ningún LLM.

## 8. Pendiente / a decidir

- Qué scrapers nuevos construir y en qué orden.
- Afinar `desired_titles.json` con la lista real y definitiva de
  títulos (el archivo actual trae un draft de partida, editable).
