"""
Módulo de scrapers — autodiscovery automático.

Cómo agregar un nuevo scraper
------------------------------
1. Crear un archivo en src/scrapers/, por ejemplo: getonboard.py
2. Exportar una función con esta firma exacta:

       def run_scraper(pages: int, keywords: list[str]) -> None:
           ...

   - pages: tope máximo de páginas (seguridad). El corte real debe
     implementarse dentro del scraper según la fecha de publicación
     (antigüedad > MAX_AGE_DAYS días).
   - keywords: lista de términos de búsqueda pasada desde el pipeline.
     Úsalas si el sitio soporta búsqueda por texto; ignóralas si usa
     categorías fijas (como Chiletrabajos).

3. Listo. load_scrapers() lo detectará automáticamente en el próximo run.

No es necesario modificar __init__.py, pipeline.py ni ningún otro archivo.

Convención de nombres
---------------------
El nombre del módulo (sin .py) se usa como identificador del scraper
en los logs del pipeline. Elige un nombre descriptivo del sitio.

Scrapers disponibles actualmente:
    - chiletrabajos  (src/scrapers/chiletrabajos.py)
"""

import importlib
import os
import pkgutil
from typing import Callable


def load_scrapers() -> dict[str, Callable]:
    """
    Descubre y carga todos los scrapers disponibles en este paquete.

    Retorna un dict {nombre: función run_scraper} para cada módulo
    en src/scrapers/ que exporte run_scraper().

    Módulos que NO exportan run_scraper() se ignoran silenciosamente
    (permite tener helpers, utils, base classes, etc. en la carpeta).

    Módulos que fallan al importarse se reportan como warning sin
    interrumpir el pipeline: los scrapers restantes siguen funcionando.
    """
    scrapers = {}
    package_dir = os.path.dirname(__file__)

    for finder, module_name, is_pkg in pkgutil.iter_modules([package_dir]):
        # Saltar el propio __init__ y módulos de soporte sin run_scraper
        if module_name.startswith("_"):
            continue

        full_name = f"src.scrapers.{module_name}"
        try:
            module = importlib.import_module(full_name)
        except Exception as e:
            print(f"[scrapers] Warning: no se pudo importar '{full_name}': {e}")
            continue

        if hasattr(module, "run_scraper") and callable(module.run_scraper):
            scrapers[module_name] = module.run_scraper
        else:
            # Módulo de soporte sin run_scraper — silencioso
            pass

    return scrapers