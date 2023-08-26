import argshell
from pathier import Pathier

from databased import DataBased, DBShell, dbparsers
from chibased import ChiBased
from griddle import griddy

root = Pathier(__file__).parent


class Chishell(DBShell):
    intro = "Starting chishell (enter help or ? for command info)..."
    prompt = "chishell>"
    dbpath: Pathier = None  # Replace None with a path to a .db file to set a default database # type: ignore

    @argshell.with_parser(dbparsers.get_create_table_parser)
    def do_add_table(self, args: argshell.Namespace):
        """Add a new table to the database."""
        with ChiBased() as db:
            db.create_table(args.table_name, args.columns)

    def do_drop_table(self, arg: str):
        """Drop the specified table."""
        with ChiBased() as db:
            db.drop_table(arg)

    @argshell.with_parser(
        dbparsers.get_add_row_parser, [dbparsers.verify_matching_length]
    )
    def do_add_row(self, args: argshell.Namespace):
        """Add a row to a table."""
        with ChiBased() as db:
            if db.add_row(args.table_name, args.values, args.columns or None):
                print(f"Added row to {args.table_name} table successfully.")
            else:
                print(f"Failed to add row to {args.table_name} table.")

    @argshell.with_parser(dbparsers.get_info_parser)
    def do_info(self, args: argshell.Namespace):
        """Print out the names of the database tables, their columns, and, optionally, the number of rows."""
        print("Getting database info...")
        with ChiBased() as db:
            tables = args.tables or db.get_table_names()
            info = [
                {
                    "Table Name": table,
                    "Columns": ", ".join(db.get_column_names(table)),
                    "Number of Rows": db.count(table) if args.rowcount else "n/a",
                }
                for table in tables
            ]
        print(DataBased.data_to_string(info))

    @argshell.with_parser(dbparsers.get_lookup_parser, [dbparsers.convert_match_pairs])
    def do_show(self, args: argshell.Namespace):
        """Find and print rows from the database.
        Use the -t/--tables, -m/--match_pairs, and -l/--limit flags to limit the search.
        Use the -c/--columns flag to limit what columns are printed.
        Use the -o/--order_by flag to order the results.
        Use the -p/--partial_matching flag to enable substring matching on -m/--match_pairs
        Pass -h/--help flag for parser help."""
        print("Finding records... ")
        if len(args.columns) == 0:
            args.columns = None
        with ChiBased() as db:
            tables = args.tables or db.get_table_names()
            for table in tables:
                results = db.get_rows(
                    table,
                    args.match_pairs,
                    columns_to_return=args.columns,
                    order_by=args.order_by,
                    limit=args.limit,
                    exact_match=not args.partial_matching,
                )
                db.close()
                print(f"{len(results)} matching rows in {table} table.")
                try:
                    print(DataBased.data_to_string(results))  # type: ignore
                except Exception as e:
                    print("Couldn't fit data into a grid.")
                    print(*results, sep="\n")
                if results:
                    print(f"{len(results)} matching rows in {table} table.")
                print()

    @argshell.with_parser(dbparsers.get_search_parser)
    def do_search(self, args: argshell.Namespace):
        """Search and return any rows containg the searched substring in any of its columns.
        Use the -t/--tables flag to limit the search to a specific table(s).
        Use the -c/--columns flag to limit the search to a specific column(s)."""
        print(f"Searching for {args.search_string}...")
        with ChiBased() as db:
            tables = args.tables or db.get_table_names()
            for table in tables:
                columns = args.columns or db.get_column_names(table)
                matcher = " OR ".join(
                    f'{column} LIKE "%{args.search_string}%"' for column in columns
                )
                query = f"SELECT * FROM {table} WHERE {matcher};"
                results = db.query(query)
                results = [db._get_dict(table, result) for result in results]
                print(f"Found {len(results)} results in {table} table.")
                print(DataBased.data_to_string(results))

    @argshell.with_parser(dbparsers.get_lookup_parser, [dbparsers.convert_match_pairs])
    def do_count(self, args: argshell.Namespace):
        """Print the number of rows in the database.
        Use the -t/--tables flag to limit results to a specific table(s).
        Use the -m/--match_pairs flag to limit the results to rows matching these criteria.
        Use the -p/--partial_matching flag to enable substring matching on -m/--match_pairs.
        Pass -h/--help flag for parser help."""
        print("Counting rows...")
        with ChiBased() as db:
            tables = args.tables or db.get_table_names()
            for table in tables:
                num_rows = db.count(table, args.match_pairs, not args.partial_matching)
                print(f"{num_rows} matching rows in {table} table.")

    def do_query(self, arg: str):
        """Execute a query against the current database."""
        print(f"Executing {arg}")
        with ChiBased() as db:
            results = db.query(arg)
        try:
            try:
                print(griddy(results))
            except Exception as e:
                for result in results:
                    print(*result, sep="|-|")
        except Exception as e:
            print(f"{type(e).__name__}: {e}")
        print(f"{db.cursor.rowcount} affected rows")

    @argshell.with_parser(dbparsers.get_update_parser, [dbparsers.convert_match_pairs])
    def do_update(self, args: argshell.Namespace):
        """Update a column to a new value.
        Two required args: the column (-c/--column) to update and the value (-v/--value) to update to.
        Use the -t/--tables flag to limit what tables are updated.
        Use the -m/--match_pairs flag to specify which rows are updated.
        Use the -p/--partial_matching flag to enable substring matching on -m/--match_pairs.
        >>> based>update -c username -v big_chungus -t users -m username lil_chungus

        ^will update the username in the users 'table' to 'big_chungus' where the username is currently 'lil_chungus'^"""
        print("Updating rows...")
        with ChiBased() as db:
            tables = args.tables or db.get_table_names()
            for table in tables:
                num_updates = db.update(
                    table,
                    args.column,
                    args.new_value,
                    args.match_pairs,
                    not args.partial_matching,
                )
                print(f"Updated {num_updates} rows in {table} table.")

    @argshell.with_parser(dbparsers.get_lookup_parser, [dbparsers.convert_match_pairs])
    def do_delete(self, args: argshell.Namespace):
        """Delete rows from the database.
        Use the -t/--tables flag to limit what tables rows are deleted from.
        Use the -m/--match_pairs flag to specify which rows are deleted.
        Use the -p/--partial_matching flag to enable substring matching on -m/--match_pairs.
        >>> based>delete -t users -m username chungus -p

        ^will delete all rows in the 'users' table whose username contains 'chungus'^"""
        print("Deleting records...")
        with ChiBased() as db:
            tables = args.tables or db.get_table_names()
            for table in tables:
                num_rows = db.delete(table, args.match_pairs, not args.partial_matching)
                print(f"Deleted {num_rows} rows from {table} table.")

    @argshell.with_parser(dbparsers.get_add_column_parser)
    def do_add_column(self, args: argshell.Namespace):
        """Add a new column to the specified tables."""
        with ChiBased() as db:
            tables = args.tables or db.get_table_names()
            for table in tables:
                db.add_column(table, args.column_name, args.type, args.default_value)

    def do_vacuum(self, arg: str):
        """Reduce database disk memory."""
        starting_size = self.dbpath.size
        print(f"Database size before vacuuming: {self.dbpath.formatted_size}")
        print("Vacuuming database...")
        with ChiBased() as db:
            freedspace = db.vacuum()
        print(f"Database size after vacuuming: {self.dbpath.formatted_size}")
        print(f"Freed up {Pathier.format_bytes(freedspace)} of disk space.")


if __name__ == "__main__":
    Chishell().cmdloop()
