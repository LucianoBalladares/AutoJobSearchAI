from src.scrapers.chiletrabajos import run_scraper
from src.filter import run_filter, init_column


def run_pipeline():
    print("=== SCRAPING ===")
    run_scraper(pages=2, keyword="data")

    print("=== FILTERING ===")
    init_column()
    run_filter()

    print("=== DONE ===")


if __name__ == "__main__":
    run_pipeline()