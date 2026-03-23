# Sistema Automatizado de Búsqueda de Empleo con IA

## 1. Objetivo

Construir un sistema automatizado que permita:

- Recolectar ofertas laborales relevantes
- Filtrarlas inteligentemente
- Priorizarlas según fit con el perfil
- Generar postulaciones personalizadas
- Optimizar el proceso de búsqueda de empleo

Meta: Maximizar entrevistas en el menor tiempo posible.

---

## 2. Arquitectura General

Pipeline:

Job Scraper → Job Database → Filtering → AI Ranking → Application Generator → Output

Salida diaria:

- Lista priorizada de empleos
- Borradores de postulación

---

## 3. Componentes del Sistema

### 3.1 Job Scraper

Función:
Recolectar ofertas desde múltiples fuentes.

Fuentes:

- LinkedIn
- Indeed
- Chiletrabajos
- Empleos Públicos
- GetOnBoard

Tecnologías:

- Python
- Requests
- BeautifulSoup
- Playwright (para sitios dinámicos)

Datos a recolectar:

- title
- company
- location
- salary
- description
- url
- date
- source

---

### 3.2 Base de Datos

Opciones:

- CSV (simple)
- SQLite (recomendado)

Estructura sugerida:

Tabla: jobs

- id
- title
- company
- location
- description
- url
- date
- source
- score

Tabla: applications

- id
- job_id
- date_applied
- status
- response

---

### 3.3 Filtering (Pre-AI)

Filtro por keywords:
\*\*Keywords temporales, cambiaran a futuro
Positivas:

- data
- analista
- bi
- salud
- hospital
- analytics

Negativas:

- senior
- ventas
- call center

---

### 3.4 AI Ranking

Objetivo:
Evaluar qué tan bien encaja cada trabajo con el perfil.

Input:

- Descripción del trabajo
- Perfil del candidato

Output:

- Score (1–10)

Ejemplo de prompt:

"Evaluate this job description for a candidate with:

- Medical Technologist
- Health Informatics training
- Power BI, SQL, Python
- English C2

Score from 1 to 10 and justify briefly."

---

### 3.6 Output

Archivo generado diariamente:

jobs_today.md

Contenido:

- Top trabajos
- Score
- Links

---

## 4. Automatización

Uso de cron (Linux):

0 8 \* \* \* python job_pipeline.py

Ejecuta diariamente:

- Scraping
- Filtrado
- Ranking

---

## 5. Flujo Diario de Uso

1. Abrir jobs_today.md
2. Revisar top trabajos
3. Ajustar CV de manera manual
4. Enviar postulaciones

---

## 6. Estructura del Proyecto

/job-search-ai

/src

- scraper.py
- filter.py
- ranker.py
- pipeline.py
- output.py

/src/scrapers

- chiletrabajos.py

/data

- jobs.db
- applications.csv

/output

- jobs_today.md

/config

- keywords.json
- profile.txt

---

## 7. Stack Tecnológico

- Python
- SQLite
- OpenAI API
- Playwright
- Pandas

---

## 8. Métricas Clave

- Número de postulaciones diarias
- Tasa de respuesta
- Tasa de entrevistas

---
