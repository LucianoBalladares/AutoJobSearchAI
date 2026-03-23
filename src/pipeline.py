from src.scrapers.chiletrabajos import run_scraper
from src.filter import run_filter, init_column as init_filter
from src.ranker import run_ranker
from src.output import run_output


def run_pipeline():
    print("=== SCRAPING ===")
    run_scraper(pages=2, keyword="data")

    print("=== FILTERING ===")
    init_filter()
    run_filter()

    print("=== RANKING ===")
    run_ranker(limit=20)

    print("=== OUTPUT ===")
    run_output(limit=10)

    print("=== DONE ===")


if __name__ == "__main__":
    run_pipeline()