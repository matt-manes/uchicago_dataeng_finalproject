import os

from noiftimer import time_it
from pathier import Pathier

root = Pathier(__file__).parent


def execute_mysql_script(sql_file: str):
    """Run the specified `sql_file`.

    This assumes that the folder containing `mysql.exe` is in your `PATH`.

    You will likely need to increase your `max_allowed_packet` parameter in the `[mysqld]` section of your cnf/ini file if it's still at the default.

    Mine is set to a value of `128M`.

    The mysql server will need to be restarted for this to take effect.

    The mysql username should be put in a file called `creds.toml` under the field `user`.

    To avoid manually typing in your password add a `pass` field to `creds.toml` with the password.

    The `.toml` file should look like (but with subbed values):
    >>> user = "username"
    >>> pass = "password"

    #### NOTE: This assumes an unimportant local mysql instance and the user accepts any security tradeoff for the convenience."""
    creds: dict = (root / "creds.toml").loads()
    if creds.get("pass"):
        os.system(f"mysql -u{creds['user']} -p{creds['pass']} < {sql_file}")
    else:
        os.system(f"mysql -u{creds['user']} -p < {sql_file}")


@time_it()
def main():
    for file in ["chidata_ddl_mysql.sql", "chidata_dml_mysql.sql", "mysql_views.sql"]:
        execute_mysql_script(file)


if __name__ == "__main__":
    main()
