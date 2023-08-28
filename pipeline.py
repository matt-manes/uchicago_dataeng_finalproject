import os

from noiftimer import time_it
from pathier import Pathier

import mysql_executor
from chibased import ChiBased
from dataloader import load_to_sqlite
from pull_data import pull

root = Pathier(__file__).parent


@time_it()
def main():
    """Run pipeline:

    * Download datasets
    * Clean/Prune
    * Create and populate sqlite database
    * Generate `chidata_dml_mysql.sql` script
    * Create chidata mysql schema
    * Insert data into mysql chidata database."""
    pull()
    load_to_sqlite()
    with ChiBased() as db:
        db.generate_mysql_dump()
    mysql_executor.main()


if __name__ == "__main__":
    main()
