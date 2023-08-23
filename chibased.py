from databased import DataBased
import pandas
import numpy
from pathier import Pathier
from typing import Any
import re

root = Pathier(__file__).parent


class ChiBased(DataBased):
    def __init__(self):
        super().__init__("chi.db")
        self.make_tables()

    def make_tables(self):
        self.create_business_licenses()
        self.create_food_inspections()
        self.create_violations()

    def drop_tables(self):
        self.drop_table("business_licenses")
        self.drop_table("food_inspections")
        self.drop_table("violations")

    def insert_many(self, table: str, values: list[tuple[Any]]):
        n = 900
        for i, row in enumerate(values):
            values[i] = [
                item.date() if isinstance(item, pandas.Timestamp) else item
                for item in row
            ]
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
                f"INSERT INTO {table} VALUES {placeholder};", flattened_values
            )

    def create_business_licenses(self):
        self.create_table(
            "business_licenses",
            [
                "id text",
                "license_id int",
                "account_number int",
                "site_number int",
                "legal_name text",
                "dba text",
                "street text",
                "city text",
                "state text",
                "zip int",
                "ward int",
                "precinct int",
                "ward_precint text",
                "police_district int",
                "license_code int",
                "license_description text",
                "business_activity_id text",
                "business_activity text",
                "license_number int",
                "application_type text",
                "application_created_date timestamp",
                "application_requirements_complete timestamp",
                "payment_date timestamp",
                "conditional_approval text",
                "license_term_start_date timestamp",
                "license_term_expiration_date timestamp",
                "license_approved_for_issuance timestamp",
                "date_issued timestamp",
                "license_status text",
                "license_status_change_date timestamp",
                "ssa int",
                "latitude real",
                "longitude real",
                "location text",
            ],
        )

    def create_food_inspections(self):
        self.create_table(
            "food_inspections",
            [
                "inspection_id int",
                "dba text",
                "aka text",
                "license_number int",
                "facility_type text",
                "risk text",
                "street text",
                "city text",
                "state text",
                "zip int",
                "inspection_date timestamp",
                "inspection_type text",
                "results text",
                "violations text",
                "latitude real",
                "longitude real",
                "location text",
            ],
        )

    def create_violations(self):
        self.create_table("violations", ["id int unique", "name text"])


def extract_violations(data: list[str]) -> list[tuple[int, str]]:
    """Extract and parse unique violations from food_inspections data to use for the violations table.

    Returns a list of tuples of the form (violation_id, violation_name)"""
    violations = {}
    for datum in data:
        # Multiple violations are delimited by ` | `
        if not datum:
            continue
        datum = datum.replace("&", "AND").split(" | ")
        for violation in datum:
            # Most violations follow the form "{number}. {name} - Comments"
            # but some don't have the "- Comments" portion
            if "Comments" in violation:
                parsed_violation = re.findall(
                    r"([0-9]{1,2})\. (.+) - Comments", violation
                )
            else:
                parsed_violation = re.findall(r"([0-9]{1,2})\. (.+)", violation)
            id_ = int(parsed_violation[0][0])
            name: str = parsed_violation[0][1]
            # Add violation to violations dict if it isn't already there
            if id_ not in violations:
                # Normalize violation name to first letter uppercase instead of all uppercase
                violations[id_] = " ".join(word.capitalize() for word in name.split())
    # Sort violations by id_ and convert to a list of tuples
    violations = [(key, violations[key]) for key in sorted(list(violations.keys()))]
    return violations


def prepare_data(
    data: pandas.DataFrame, date_columns: list[str], date_format: str = "%m/%d/%Y"
) -> pandas.DataFrame:
    """Convert `NaN` values in `data` to `None` and convert `date_columns` values to `DataBased` compatible strings.

    #### `date_format`: The strftime format of the `date_columns` values.

    Returns the dataframe."""
    data = data.fillna(numpy.nan).replace([numpy.nan], [None])
    for column in date_columns:
        # Convert to datetime
        data[column] = pandas.to_datetime(data[column], format=date_format)
        # Convert back to a string in a `Databased` compatible format
        data[column] = data[column].dt.strftime("%Y-%m-%d %H:%M:%S")
    return data


def load_data_to_db():
    """Load data from `.csv` files into the `sqlite` database."""
    with ChiBased() as db:
        db.dbpath.backup()
        db.drop_tables()
        db.vacuum()
        db.make_tables()
        for target in [
            (
                "business_licenses",
                [
                    "APPLICATION CREATED DATE",
                    "APPLICATION REQUIREMENTS COMPLETE",
                    "PAYMENT DATE",
                    "LICENSE TERM START DATE",
                    "LICENSE TERM EXPIRATION DATE",
                    "LICENSE APPROVED FOR ISSUANCE",
                    "DATE ISSUED",
                    "LICENSE STATUS CHANGE DATE",
                ],
            ),
            ("food_inspections", ["Inspection Date"]),
        ]:
            data = pandas.read_csv(f"{target[0]}.csv")
            data = prepare_data(data, target[1])
            if target[0] == "food_inspections":
                db.insert_many(
                    "violations", extract_violations(data["Violations"].values.tolist())
                )
            db.insert_many(target[0], data.values.tolist())


if __name__ == "__main__":
    load_data_to_db()
