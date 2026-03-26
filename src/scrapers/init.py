"""
Paquete de scrapers para AutoJobSearchAI.

Este archivo cumple dos roles:
1. Declara src/scrapers/ como paquete Python.
2. Expone load_scrapers() como símbolo público del paquete, de modo que
   `from src.scrapers import load_scrapers` funcione sin importar el
   módulo interno por nombre.

Convención para scrapers nuevos
--------------------------------
Cada archivo en este directorio debe exportar una función con esta firma:

    def run_scraper(pages: int, keywords: list[str]) -> None

load_scrapers() la descubre automáticamente. No es necesario tocar
pipeline.py ni este archivo al agregar una nueva fuente.

Ejemplo mínimo (src/scrapers/mi_fuente.py):

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
    se loguea el error y se omite ese scraper sin interrumpir los demás.

    Este símbolo se exporta explícitamente desde el paquete para que
    `from src.scrapers import load_scrapers` funcione de forma directa.
    """
    scrapers: dict[str, Callable] = {}
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


# Exportación explícita del símbolo público de este paquete.
# Cualquier import del estilo `from src.scrapers import load_scrapers`
# resuelve aquí sin necesidad de conocer el módulo interno.
__all__ = ["load_scrapers"]