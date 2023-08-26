from noiftimer import time_it
from dataloader import load_to_sqlite
from pull_data import pull


@time_it()
def main():
    """Run pipeline:

    * Download datasets
    * Clean/Prune
    * Create sqlite database"""
    pull()
    load_to_sqlite()


if __name__ == "__main__":
    main()
