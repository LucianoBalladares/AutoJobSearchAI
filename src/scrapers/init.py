"""
Interfaz estándar para scrapers de AutoJobSearchAI.

Convención: cada scraper en este directorio debe exportar una función
con la siguiente firma exacta:

    def run_scraper(pages: int, keywords: list[str]) -> None

pipeline.py usa load_scrapers() para descubrir y ejecutar todos los
scrapers disponibles sin necesidad de imports hardcodeados. Para agregar
un scraper nuevo basta con crear el archivo y seguir la convención.

Ejemplo de scraper mínimo (src/scrapers/mi_fuente.py):

    from src.db import init_db, get_connection
    from datetime import datetime

    def run_scraper(pages=2, keywords=None):
        if keywords is None:
            keywords = ["data"]
        init_db()
        # ... lógica de scraping ...
"""

import importlib
import pkgutil
from pathlib import Path
from typing import Callable


def load_scrapers() -> dict[str, Callable]:
    """
    Descubre y carga todos los módulos en src/scrapers/ que exporten
    run_scraper(). Retorna un dict {nombre: función}.

    Si un módulo falla al importar (dependencia faltante, error de sintaxis),
    se logea el error y se omite ese scraper sin interrumpir los demás.
    """
    scrapers = {}
    package_path = Path(__file__).parent
    package_name = __name__  # "src.scrapers"

    for module_info in pkgutil.iter_modules([str(package_path)]):
        module_name = f"{package_name}.{module_info.name}"
        try:
            module = importlib.import_module(module_name)
        except Exception as e:
            print(f"[scrapers] No se pudo cargar {module_name}: {e}")
            continue

        if hasattr(module, "run_scraper") and callable(module.run_scraper):
            scrapers[module_info.name] = module.run_scraper
        else:
            print(f"[scrapers] {module_name} no exporta run_scraper(), ignorado.")

    return scrapers