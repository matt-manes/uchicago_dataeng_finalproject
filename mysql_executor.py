import getpass
import os

from noiftimer import time_it
from pathier import Pathier

root = Pathier(__file__).parent


def load_creds() -> dict[str, str] | None:
    """Read and return `creds.toml` if it exists, otherwise return `None`.

    The mysql username should be put in a file called `creds.toml` under the field `username`.

    To avoid manually typing in your password add a `password` field to `creds.toml` with the password.

    The `.toml` file should look like (but with subbed values):
    >>> username = <username>
    >>> password = <password>

    #### NOTE: This assumes an unimportant local mysql instance and the user accepts any security tradeoff for the convenience."""
    file = root / "creds.toml"
    if file.exists():
        return file.loads()
    return None


def prompt_for_creds() -> dict[str, str]:
    """Prompt the user for their username and password."""
    creds = {}
    creds["username"] = input("Enter MySQL username: ")
    creds["password"] = getpass.getpass("Enter MySQL password: ")
    return creds


def get_creds() -> dict[str, str]:
    """Try to load creds from `creds.toml`.

    If it doesn't exist, prompt the user."""
    creds = load_creds()
    if not creds:
        creds = prompt_for_creds()
    return creds


def execute_mysql_script(sql_file: str, creds: dict[str, str] | None):
    """Run the specified `sql_file`.

    This assumes that the folder containing `mysql.exe` is in your `PATH`.

    You will likely need to increase your `max_allowed_packet` parameter in the `[mysqld]` section of your cnf/ini file if it's still at the default.

    Mine is set to a value of `128M`.

    The mysql server will need to be restarted for this to take effect."""
    if not creds:
        creds = get_creds()
    os.system(f"mysql -u{creds['username']} -p{creds['password']} < {sql_file}")


@time_it()
def main():
    creds = get_creds()
    for file in ["chidata_ddl_mysql.sql", "chidata_dml_mysql.sql", "mysql_views.sql"]:
        execute_mysql_script(file, creds)


if __name__ == "__main__":
    main()
