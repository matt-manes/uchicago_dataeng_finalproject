from databased import DataBased, _connect
from pathier import Pathier
from typing import Any
import sqlite3

root = Pathier(__file__).parent


class ChiBased(DataBased):
    def __init__(self):
        super().__init__("chi.db")

    def open(self):
        self.connection = sqlite3.connect(
            self.dbpath,
            # detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
            timeout=self.connection_timeout,
        )
        self.connection.execute("pragma foreign_keys = 1;")
        self.cursor = self.connection.cursor()
        self.connection_open = True

    @_connect
    def create_tables_script(self):
        """Create tables from `chi_tables.sql` script.

        #### Will drop table first if it exists."""
        self.connection.executescript((root / "chidata_ddl_sqlite.sql").read_text())
        self.vacuum()

    def insert_many(self, table: str, columns: list[str], values: list[tuple[Any]]):
        """Insert rows in chunks."""
        n = 900
        columns = "(" + ", ".join(columns) + ")"
        for i in range(0, len(values), n):
            chunk = values[i : i + n]
            placeholder = (
                "("
                + "),(".join(", ".join("?" for _ in value_set) for value_set in chunk)
                + ")"
            )
            flattened_values = tuple(
                value for value_set in chunk for value in value_set
            )
            self.cursor.execute(
                f"INSERT INTO {table} {columns} VALUES {placeholder};", flattened_values
            )
