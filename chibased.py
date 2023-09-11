import sqlite3
from typing import Any

from databased import DataBased, _connect
from noiftimer import time_it
from pathier import Pathier

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

    @time_it()
    def generate_mysql_dump(self):
        """Generate a file called `chidata_dml_mysql.sql` that contains insert statements for all of the data."""
        output = "USE chidata;\n"
        output += "SET foreign_key_checks = 0;\n"
        indent = "    "
        for table in self.get_table_names():
            output += f"TRUNCATE TABLE {table};\n"
            columns = ", ".join(self.get_column_names(table))
            output += "INSERT INTO\n"
            output += f"{indent}{table} ({columns})\n"
            output += "VALUES\n"
            rows = self.get_rows(table, values_only=True)
            rows = [tuple([value or "NULL" for value in row]) for row in rows]
            output += ",\n".join(f"{indent}{row}" for row in rows)
            output += ";\n"
        output += "SET foreign_key_checks = 1;"
        output = output.replace("'NULL'", "NULL")
        (root / "chidata_dml_mysql.sql").write_text(output, encoding="utf-8")


if __name__ == "__main__":
    with ChiBased() as db:
        db.generate_mysql_dump()
