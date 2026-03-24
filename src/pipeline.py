from src.scrapers.chiletrabajos import run_scraper
from src.filter import run_filter
from src.ranker import run_ranker, init_column as init_ranker
from src.output import run_output


def run_pipeline():
    try:
        print("=== INIT ===")
        init_ranker()

        print("=== SCRAPING ===")
        run_scraper(pages=2, keyword="data")

        print("=== FILTERING ===")
        run_filter()

        print("=== RANKING ===")
        run_ranker(limit=20)

        print("=== OUTPUT ===")
        run_output(limit=10)

        print("=== DONE ===")

    except Exception as e:
        print(f"Pipeline failed: {e}")


if __name__ == "__main__":
    run_pipeline()