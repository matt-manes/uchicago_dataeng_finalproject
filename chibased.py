import sqlite3
from typing import Any

from databased import Databased
from noiftimer import time_it
from pathier import Pathier

root = Pathier(__file__).parent


class ChiBased(Databased):
    def __init__(self):
        super().__init__("chi.db", detect_types=False)

    def create_tables_script(self):
        """Create tables from `chi_tables.sql` script.

        #### Will drop table first if it exists."""
        self.execute_script((root / "chidata_ddl_sqlite.sql"))
        self.vacuum()

    @time_it()
    def generate_mysql_dump(self):
        """Generate a file called `chidata_dml_mysql.sql` that contains insert statements for all of the data."""
        output = "USE chidata;\n"
        output += "SET foreign_key_checks = 0;\n"
        indent = "    "
        for table in self.tables:
            output += f"TRUNCATE TABLE {table};\n"
            columns = ", ".join(self.get_columns(table))
            output += "INSERT INTO\n"
            output += f"{indent}{table} ({columns})\n"
            output += "VALUES\n"
            rows = [list(row.values()) for row in self.select(table)]
            rows = [tuple([value or "NULL" for value in row]) for row in rows]
            output += ",\n".join(f"{indent}{row}" for row in rows)
            output += ";\n"
        output += "SET foreign_key_checks = 1;"
        output = output.replace("'NULL'", "NULL")
        (root / "chidata_dml_mysql.sql").write_text(output, encoding="utf-8")


if __name__ == "__main__":
    with ChiBased() as db:
        db.generate_mysql_dump()
