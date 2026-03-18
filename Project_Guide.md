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

### 3.5 Application Generator

Función:
Generar automáticamente:
- Cover letter
- Email
- Mensaje LinkedIn

Input:
- CV
- Job description

Output:
- Texto listo para enviar

Prompt ejemplo:

"Write a concise 150-word cover letter tailored to this job using the candidate profile."

---

### 3.6 Output

Archivo generado diariamente:

jobs_today.md

Contenido:
- Top 10 trabajos
- Score
- Links
- Borradores de postulación

---

## 4. Automatización

Uso de cron (Linux):

0 8 * * * python job_pipeline.py

Ejecuta diariamente:
- Scraping
- Filtrado
- Ranking
- Generación

---

## 5. Flujo Diario de Uso

1. Abrir jobs_today.md
2. Revisar top trabajos
3. Ajustar CV si es necesario
4. Enviar postulaciones

Tiempo estimado:
15–25 minutos diarios

---

## 6. Estructura del Proyecto

/job-search-ai

/src
- scraper.py
- filter.py
- ranker.py
- generator.py
- pipeline.py

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

## 9. Roadmap de Implementación

Día 1:
- Scraper básico

Día 2:
- Base de datos + filtering

Día 3:
- AI ranking

Día 4:
- Generador de postulaciones

Día 5:
- Automatización

---

## 10. Consideraciones Finales

- No sobre-ingenierizar
- Priorizar velocidad de ejecución
- Iterar rápidamente
- Medir resultados

---

Este sistema está diseñado para maximizar eficiencia y volumen de postulaciones de alta calidad en el menor tiempo posible.

