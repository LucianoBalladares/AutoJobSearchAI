# AutoJobSearchAI — Guía del Proyecto

## 1. Objetivo

Sistema automatizado que:

- Recolecta ofertas laborales desde múltiples fuentes.
- Elimina duplicados (URL única, cross-fuente).
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
depende de la descripción completa del job ni de ninguna API externa
de IA.

## 3. Arquitectura

```
Scrapers (título, empresa, ubicación, link — sin descripción completa)
        ↓
DB SQLite (url UNIQUE → dedup automático, cross-fuente)
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
No hace falta tocar `pipeline.py` para agregar uno nuevo. Módulos que
empiezan con `_` (como `_common.py`) se ignoran — es donde vive la
lógica compartida (`save_job`, `get_existing_urls`, headers rotativos)
para no duplicarla en cada scraper.

**Fuentes activas:**

- **Chiletrabajos** — categorías fijas (`informatica`, `medicina`,
  `administracion`, `ingenieria`, `asistenteadministrativo`), ignora
  `keywords`. No visita la página de detalle de cada oferta — causaba
  fallos frecuentes y era el cuello de botella de velocidad. Fecha
  real del sitio: formato `"DD de Mes de YYYY"` (ej. "15 de Junio de
  2026"), no `DD/MM/YYYY` — confirmado contra el sitio en vivo.
  Confianza: alta.

- **Computrabajo Chile** (`cl.computrabajo.com`) — búsqueda por texto,
  usa `keywords` (una request por título en `desired_titles.json`).
  Selectores HTML confirmados contra un scraper de Computrabajo real
  en producción (mismo template en todos los países del grupo). No
  visita página de detalle, mismo criterio que Chiletrabajos.
  Confianza: alta.

- **GetOnBoard** — a diferencia de las otras dos, consume su **API
  pública oficial** (`https://www.getonbrd.com/api/v0`) en vez de
  HTML. Confirmado en el código fuente de su librería oficial en Ruby
  que el facet público no requiere autenticación. Usa `keywords` vía
  `GET /search/jobs?query=<término>`. La extracción de campos
  (título, empresa, url, fecha) es defensiva — prueba varios nombres
  candidatos por campo, porque no fue posible confirmar en vivo el
  JSON exacto de respuesta sin acceso de red al dominio durante el
  desarrollo. Si el mapeo falla, el propio scraper imprime el JSON
  crudo del primer resultado sin procesar para diagnóstico rápido.
  Confianza: media-alta (API confirmada, campos parcialmente
  verificados).

**Descartadas deliberadamente:**

- **LinkedIn** — requiere sesión autenticada para ver resultados
  completos. Usar la cuenta personal para scraping automatizado viola
  el User Agreement de LinkedIn explícitamente y arriesga un baneo de
  cuenta. No vale la pena el riesgo para una herramienta de uso
  personal.
- **Indeed** — protección Cloudflare fuerte y, al igual que LinkedIn,
  requiere sesión para resultados completos.
- **Alternativa considerada, no construida**: ambos sitios permiten
  crear alertas de búsqueda por correo. Un parser IMAP de esas alertas
  conseguiría datos de LinkedIn/Indeed sin tocar sus sitios ni violar
  ningún ToS — es una arquitectura completamente distinta (no es un
  "scraper" en el sentido de este proyecto) y no se ha implementado.

**Fuentes evaluadas, no construidas aún:**

- **Empleos Públicos** (`empleospublicos.cl`) — portal oficial de
  empleo en el Estado chileno, relevante dado el perfil de informática
  en salud (roles en Servicios de Salud / MINSAL). Riesgo técnico bajo
  (sitio de gobierno). Prioridad media.
- **BNE — Bolsa Nacional de Empleo** (`bne.cl`) — portal oficial más
  amplio (no solo sector público). Hay indicios de que buscar/ver
  ofertas podría requerir cuenta registrada, no solo postular — no
  confirmado. Antes de construirlo hay que verificar si se puede
  navegar sin login.
- **Laborum / Trabajando.com** — según BNE, ya están siendo
  integrados/enlazados dentro de ese portal. Probablemente redundantes
  si se construye BNE.

### 4.2 Base de datos (`src/db.py`)

Tabla `jobs`:

| columna      | tipo        | uso                                                              |
| ------------ | ----------- | ---------------------------------------------------------------- |
| id           | INTEGER PK  | —                                                                |
| title        | TEXT        | título de la oferta                                              |
| company      | TEXT        | empresa                                                          |
| location     | TEXT        | ubicación                                                        |
| description  | TEXT        | vacío en la práctica — ningún scraper activo visita detalle      |
| url          | TEXT UNIQUE | dedup, cross-fuente                                              |
| date         | TEXT        | fecha tal como la muestra el sitio de origen                     |
| source       | TEXT        | nombre del scraper (`chiletrabajos`, `computrabajo`, `getonbrd`) |
| created_at   | TEXT        | timestamp de scraping — base de la retención de 7 días           |
| filtered     | INTEGER     | 1 = título coincide con desired_titles.json                      |
| delivered_at | TEXT        | timestamp de cuándo se incluyó en un reporte                     |

La columna `score` (del ranking con IA) fue eliminada. `init_db()`
migra automáticamente bases de datos antiguas que aún la tengan.
`get_connection()` crea `data/` si no existe — necesario porque esa
carpeta está gitignored y no existe en un clone fresco.

### 4.3 Filtro (`src/filter.py`)

Compara el título normalizado (sin acentos, minúsculas) contra
`config/desired_titles.json`:

1. Si contiene algún `exclude_terms` (palabra completa) → rechazado,
   sin importar lo demás.
2. Si no, y contiene algún `desired_titles` (substring) → aceptado.
3. Si no matchea nada → rechazado.

Opera solo sobre el título, no sobre la descripción (que en la
práctica está vacía — ver 4.2).

La lista actual de `desired_titles` está armada a partir del CV real
(Healthcare Data Analyst / Health Informatics / BI / Data Engineering),
no es un placeholder genérico.

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
    _common.py
    chiletrabajos.py
    computrabajo.py
    getonbrd.py

/config
  desired_titles.json
  state.json          (autogenerado)

/data
  jobs.db             (gitignored, se crea sola)

/output
  jobs_*.md           (gitignored)
```

## 7. Stack

- Python
- SQLite
- Requests + BeautifulSoup (Chiletrabajos, Computrabajo)
- Requests puro contra API JSON (GetOnBoard)
- tenacity (reintentos HTTP en los tres scrapers)

Sin dependencia de OpenAI ni de ningún LLM.

## 8. Pendiente / a decidir

- **Confirmar el mapeo de campos de GetOnBoard** contra una corrida
  real (`python3 -m src.scrapers.getonbrd`) — es lo único del proyecto
  que no quedó verificado en vivo antes de entregarse.
- Evaluar Empleos Públicos y BNE (confirmar si BNE permite navegar sin
  login antes de invertir tiempo ahí).
- Decidir si construir el parser de alertas por correo (IMAP) para
  LinkedIn/Indeed, o dejarlo descartado definitivamente.
- Revisar la interacción entre `exclude_terms` y `desired_titles`: un
  término genérico como "ingeniero" en `exclude_terms` excluye también
  "Ingeniero de Datos" si ese título está en la lista de deseados — no
  hay lógica de excepción por frase completa, es exclusión por palabra
  suelta en todo el título.
