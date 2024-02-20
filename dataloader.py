import re
from typing import Any

import numpy
import pandas
from noiftimer import time_it
from pathier import Pathier
from younotyou import younotyou

from chibased import ChiBased

root = Pathier(__file__).parent
licenses_path = root / "business_licenses.csv"
inspections_path = root / "food_inspections.csv"
pandas.set_option("mode.chained_assignment", None)
""" Functions for reading, cleaning, and normalizing csv data. """


class BusinessLicenses:
    def __init__(self):
        self.csv_path = licenses_path

    @time_it()
    def load(self) -> pandas.DataFrame:
        return pandas.read_csv(self.csv_path)

    def get_id_lookup_table(
        self, table: str, id_column: str, match_column: str
    ) -> dict[Any, int]:
        """Get a dictionary for `table` with `match_column` values as keys and `id_column` values as values.

        e.g.
        >>> addy = self.get_id_lookup_table("addresses", "address_id", "street")

        `addy` can then be used to look up the `address_id` for `123 street`.

        Faster at scale than looking up ids individually from the database."""
        with ChiBased() as db:
            return {
                row[match_column]: row[id_column]
                for row in db.select(table, [id_column, match_column])
            }

    def replace_column_with_id(
        self,
        data: pandas.DataFrame,
        frame_column: str,
        frame_column_new_name: str,
        lookup_table: str,
        lookup_id_column: str,
        lookup_match_column: str,
    ) -> pandas.DataFrame:
        """Replace and rename a column in `data` with the corresponding id for its value.

        #### :params:
        * `frame_column`: The column in `data` to be replaced and renamed.
        * `frame_column_new_name`: The new name for the column.
        * `lookup_table`: The table in the database to use for generating the replacement lookup dict.
        * `lookup_id_column`: The column in the database table to serve as the id value.
        * `lookup_match_column`: The column in the database table corresponding to the `data` column that is getting replaced.

        >>> data = self.replace_column_with_id(data, "street", "address_id", "addresses", "address_id", "street")

        will return a new dataframe where the `street` column has been renamed to `address_id` and the values replaced with id numbers.
        """
        lookup = self.get_id_lookup_table(
            lookup_table, lookup_id_column, lookup_match_column
        )
        data[frame_column] = data[frame_column].apply(lambda key: lookup[key])
        data = data.rename({frame_column: frame_column_new_name})
        return data

    @time_it()
    def rename_columns(self, data: pandas.DataFrame) -> pandas.DataFrame:
        """Rename columns."""
        return data.rename(
            columns={
                "ID": "id",
                "LICENSE ID": "application_id",
                "ACCOUNT NUMBER": "account_number",
                "SITE NUMBER": "site_number",
                "LEGAL NAME": "legal_name",
                "DOING BUSINESS AS NAME": "dba",
                "ADDRESS": "street",
                "CITY": "city",
                "STATE": "state",
                "ZIP CODE": "zip",
                "WARD": "ward",
                "PRECINCT": "precinct",
                "WARD PRECINCT": "ward_precinct",
                "POLICE DISTRICT": "police_district",
                "LICENSE CODE": "license_code",
                "LICENSE DESCRIPTION": "license_description",
                "BUSINESS ACTIVITY ID": "business_activity_id",
                "BUSINESS ACTIVITY": "business_activity",
                "LICENSE NUMBER": "license_number",
                "APPLICATION TYPE": "application_type",
                "APPLICATION CREATED DATE": "application_created_date",
                "APPLICATION REQUIREMENTS COMPLETE": "application_requirements_complete_date",
                "PAYMENT DATE": "payment_date",
                "CONDITIONAL APPROVAL": "conditional_approval",
                "LICENSE TERM START DATE": "license_term_start_date",
                "LICENSE TERM EXPIRATION DATE": "license_term_expiration_date",
                "LICENSE APPROVED FOR ISSUANCE": "license_approved_for_issuance_date",
                "DATE ISSUED": "issue_date",
                "LICENSE STATUS": "license_status",
                "LICENSE STATUS CHANGE DATE": "license_status_change_date",
                "SSA": "ssa",
                "LATITUDE": "latitude",
                "LONGITUDE": "longitude",
                "LOCATION": "location",
            }
        )

    @time_it()
    def fill_missing(self, data: pandas.DataFrame) -> pandas.DataFrame:
        """Fill missing values in `data` with `None`."""
        return data.fillna(numpy.nan).replace(numpy.nan, None)

    @time_it()
    def remove_non_chicago_entries(self, data: pandas.DataFrame) -> pandas.DataFrame:
        """Remove rows without a ward number."""
        data = data.dropna(subset=["ward"])
        data = data[data["state"] == "IL"]
        # Schiller Park is a suburb and not in ward 3,
        # but a couple entries list it as such
        return data[data["city"] != "SCHILLER PARK"]

    @time_it()
    def drop_columns(self, data: pandas.DataFrame) -> pandas.DataFrame:
        """Drop unneeded columns."""
        return data.drop(
            columns=[
                "id",
                "city",
                "state",
                "ward_precinct",
                "precinct",
                "police_district",
                "business_activity_id",
                "business_activity",
                "application_type",
                "application_created_date",
                "application_requirements_complete_date",
                "payment_date",
            ]
        )

    @time_it()
    def convert_dates(self, data: pandas.DataFrame) -> pandas.DataFrame:
        """Convert dates to `%Y-%m-%d` format."""

        def converter(date: str) -> str | None:
            if not isinstance(date, str):
                return date
            m, d, y = date.split("/")
            return f"{y}-{m}-{d}"

        for column in data.columns:
            if "_date" in column:
                data[column] = data[column].apply(converter)
        return data

    @time_it()
    def normalize_strings(self, data: pandas.DataFrame) -> pandas.DataFrame:
        """Convert multi-whitespaces to single whitespaces and convert words to first letter capitals."""
        normalize = lambda string_: (
            " ".join(word.capitalize() for word in string_.split())
            if isinstance(string_, str)
            else string_
        )
        for column in data.columns:
            if column != "license_status":
                data[column] = data[column].apply(normalize)
        return data

    @time_it()
    def insert_address_data(self, data: pandas.DataFrame):
        """Populate `business_addresses` table."""
        # Get unique addresses
        # Some entries don't have a `location` field,
        # weed out duplicates for those based on `street` only
        data_no_location = data[data["location"].isnull()]
        data_no_location = data_no_location.drop_duplicates(subset=["street"])
        # Drop no location rows
        data = data.dropna(subset=["location"])
        data = data.drop_duplicates(subset=["street", "location"])
        # Put the two frames back together
        data = pandas.concat([data, data_no_location])
        data = data.sort_values(["ward", "street"])
        addresses = data[
            [
                "street",
                "zip",
                "ward",
                "latitude",
                "longitude",
            ]
        ]
        with ChiBased() as db:
            db.insert(
                "business_addresses", list(addresses.columns), addresses.values.tolist()
            )

    @time_it()
    def insert_businesses_data(self, data: pandas.DataFrame):
        """Populate `businesses` table."""
        # Get unique businesses based on `account_number`
        data = data.drop_duplicates(["account_number"])
        data = data.sort_values(["account_number"])
        data = data[["account_number", "legal_name", "dba", "street"]]
        businesses = self.replace_column_with_id(
            data, "street", "address_id", "business_addresses", "id", "street"
        )
        with ChiBased() as db:
            db.insert(
                "businesses",
                ["account_number", "legal_name", "dba", "address_id"],
                businesses.values.tolist(),
            )

    @time_it()
    def insert_license_code_data(self, data: pandas.DataFrame):
        """Populate `license_codes` table."""
        # Get unique values based on `license_code`
        data = data.drop_duplicates(["license_code"])
        data = data.sort_values(["license_code"])
        data = data[["license_code", "license_description"]]
        with ChiBased() as db:
            db.insert("license_codes", ["code", "description"], data.values.tolist())

    @time_it()
    # The prefixed `_` is to prevent this from running in `self.load_data_to_db` to reduce db size
    def _insert_application_type_data(self, data: pandas.DataFrame):
        """Populate `application_types` table."""
        # Get unique values
        application_types = data.drop_duplicates(subset=["application_type"])[
            ["application_type"]
        ].values.tolist()
        with ChiBased() as db:
            db.insert("application_types", ["type"], application_types)

    @time_it()
    # The prefixed `_` is to prevent this from running in `self.load_data_to_db` to reduce db size
    def _insert_application_data(self, data: pandas.DataFrame):
        """Populate `license_applications` and `application_payments` tables."""
        data = data[
            [
                "application_id",
                "license_number",
                "license_code",
                "account_number",
                "application_type",  # Get type_id from `application_types` table
                "payment_date",  # Concurrently populate `application_payments` table so generated `payment_id` stays consistent
                "application_created_date",
                "application_requirements_complete_date",
                "license_approved_for_issuance_date",
                "conditional_approval",
                "site_number",
            ]
        ].sort_values(["application_id"])
        payments = data["payment_date"]
        data = data.drop(columns=["payment_date"])
        with ChiBased() as db:
            # Replace values in `application_types` with corresponding `application_id`
            data = self.replace_column_with_id(
                data,
                "application_type",
                "application_id",
                "application_types",
                "id",
                "type",
            )
            # Generate `payment_id` data so it's consistent across the two tables
            payment_id = 1
            payment_dates = []
            applications = []
            for application, payment in zip(
                data.values.tolist(), payments.values.tolist()
            ):
                applications.append(tuple(list(application) + [payment_id]))
                payment_dates.append(tuple([payment_id, payment]))
                payment_id += 1
            db.insert("application_payments", ["id", "date"], payment_dates)
            db.insert(
                "license_applications",
                [
                    "id",
                    "license_number",
                    "license_code",
                    "account_number",
                    "application_type_id",
                    "created_date",
                    "completed_date",
                    "approval_date",
                    "conditional_approval",
                    "site_number",
                    "payment_id",
                ],
                applications,
            )

    @time_it()
    def insert_license_status_data(self, data: pandas.DataFrame):
        """Populate `license_statuses` table."""
        with ChiBased() as db:
            db.insert(
                "license_statuses",
                ["id", "status", "description"],
                [
                    (1, "AAI", "License Issued"),
                    (2, "AAC", "Cancelled During Term"),
                    (3, "REV", "Revoked"),
                    (4, "REA", "Revocation Appealed"),
                    (
                        5,
                        "INQ",
                        "???",
                    ),  # The data source doesn't specify what this means and there' only one instance of it
                ],
            )

    @time_it()
    def insert_license_data(self, data: pandas.DataFrame):
        """Populate `licenses` table."""
        data = data[
            [
                "license_number",
                "account_number",
                "license_term_start_date",
                "license_term_expiration_date",
                "issue_date",
                "license_status",
                "license_status_change_date",
                "license_code",
            ]
        ]
        data = data.sort_values("license_term_start_date", ascending=False)
        data = data.drop_duplicates(["license_number"])
        with ChiBased() as db:
            licenses = self.replace_column_with_id(
                data,
                "license_status",
                "status_id",
                "license_statuses",
                "id",
                "status",
            )
            licenses = licenses.sort_values("license_number")
            db.insert(
                "licenses",
                [
                    "license_number",
                    "account_number",
                    "start_date",
                    "expiration_date",
                    "issue_date",
                    "status_id",
                    "status_change_date",
                    "license_code",
                ],
                licenses.values.tolist(),
            )

    @time_it()
    def prepare_data(self) -> pandas.DataFrame:
        """Run preparation pipeline and return dataframe."""
        data = self.load()
        data = self.rename_columns(data)
        data = self.remove_non_chicago_entries(data)
        data = self.drop_columns(data)
        data = self.normalize_strings(data)
        data = self.convert_dates(data)
        data = self.fill_missing(data)
        return data

    @time_it()
    def load_data_to_db(self):
        """Prepare and insert data into sqlite database."""
        data = self.prepare_data()
        # Get and execute all data insertion functions in this class
        # (I keep forgetting to add each one here after I write it
        # and then wonder why the data didn't show up in the database)
        # (NOTE: This means the order of the functions in the class matters)
        # `self.__class__.__base__().__dir__()` is so child classes don't call parent class data insertion functions
        for func in younotyou(
            self.__dir__(), ["insert_*_data"], self.__class__.__base__().__dir__()  # type: ignore
        ):
            getattr(self, func)(data)


class FoodInspections(BusinessLicenses):
    def __init__(self):
        self.csv_path = inspections_path

    @time_it()
    def rename_columns(self, data: pandas.DataFrame) -> pandas.DataFrame:
        return data.rename(
            columns={
                "Inspection ID": "inspection_id",
                "DBA Name": "dba",
                "AKA Name": "aka",
                "License #": "license_number",
                "Facility Type": "facility_type",
                "Risk": "risk",
                "Address": "street",
                "City": "city",
                "State": "state",
                "Zip": "zip",
                "Inspection Date": "inspection_date",
                "Inspection Type": "inspection_type",
                "Results": "results",
                "Violations": "violations",
                "Latitude": "latitude",
                "Longitude": "longitude",
                "Location": "location",
            }
        )

    @time_it()
    def fix_cities(self, data: pandas.DataFrame) -> pandas.DataFrame:
        """Some of these inspectors/data entriest are sloppy as hell with city names."""
        data["city"] = data["city"].replace(
            [
                "Cchicago",
                "Chicago.",
                "Chicagochicago",
                "312chicago",
                "Chicagoc",
                "Chicagoo",
            ],
            "Chicago",
        )
        return data

    @time_it()
    def remove_non_chicago_entries(self, data: pandas.DataFrame) -> pandas.DataFrame:
        """Remove rows with city values that aren't 'Chicago'."""
        return data[data["city"] == "Chicago"]

    @time_it()
    def remove_schools(self, data: pandas.DataFrame) -> pandas.DataFrame:
        """Remove school facilities. (They seem to be operating under a different licensing scheme.)"""
        return data[~data["facility_type"].str.contains("School", na=False)]

    @time_it()
    def insert_facility_type_data(self, data: pandas.DataFrame):
        """Populate `facility_types` table."""
        facility_types = (
            data[["facility_type"]].drop_duplicates().sort_values("facility_type")
        )
        with ChiBased() as db:
            db.insert("facility_types", ["name"], facility_types.values.tolist())

    @time_it()
    def insert_risk_level_data(self, data: pandas.DataFrame):
        """Populate `risk_levels` table."""
        risk_levels = data[["risk"]].drop_duplicates().sort_values("risk")
        with ChiBased() as db:
            db.insert("risk_levels", ["name"], risk_levels.values.tolist())

    @time_it()
    def insert_facility_address_data(self, data: pandas.DataFrame):
        """Populate `facility_addresses` table."""
        data = (
            data[["street", "zip", "latitude", "longitude", "facility_type", "risk"]]
            .drop_duplicates(["street"])
            .sort_values("street")
        )
        for args in [
            [
                "facility_type",
                "facility_type_id",
                "facility_types",
                "id",
                "name",
            ],
            ["risk", "risk_id", "risk_levels", "id", "name"],
        ]:
            data = self.replace_column_with_id(data, *args)
        with ChiBased() as db:
            db.insert(
                "facility_addresses",
                [
                    "street",
                    "zip",
                    "latitude",
                    "longitude",
                    "facility_type_id",
                    "risk_id",
                ],
                data.values.tolist(),
            )

    @time_it()
    def insert_inspected_business_data(self, data: pandas.DataFrame):
        """Populate `inspected_businesses` table."""
        data = (
            data[["license_number", "dba", "aka"]]
            .drop_duplicates("license_number")
            .dropna(subset=["license_number"])
            .sort_values("license_number")
        )
        with ChiBased() as db:
            db.insert(
                "inspected_businesses",
                ["license_number", "dba", "aka"],
                data.values.tolist(),
            )

    @time_it()
    def insert_inspection_type_data(self, data: pandas.DataFrame):
        """Populate `inspection_types` table."""
        data = (
            data[["inspection_type"]]
            .drop_duplicates("inspection_type")
            .sort_values("inspection_type")
        )
        with ChiBased() as db:
            db.insert("inspection_types", ["name"], data.values.tolist())

    @time_it()
    def insert_result_type_data(self, data: pandas.DataFrame):
        """Populate `result_types` table."""
        data = data[["results"]].drop_duplicates("results").sort_values("results")
        with ChiBased() as db:
            db.insert("result_types", ["description"], data.values.tolist())

    @time_it()
    def insert_inspection_data(self, data: pandas.DataFrame):
        """Populate `inspections` table."""
        data = data[
            [
                "inspection_id",
                "license_number",
                "street",
                "inspection_type",
                "results",
                "inspection_date",
            ]
        ].sort_values("inspection_id")
        data = data.dropna(subset=["license_number"])
        data = data.drop_duplicates(
            subset=["license_number", "inspection_type", "results", "inspection_date"]
        )
        for args in [
            ["street", "facility_address_id", "facility_addresses", "id", "street"],
            ["inspection_type", "inspection_type_id", "inspection_types", "id", "name"],
            ["results", "result_type_id", "result_types", "id", "description"],
        ]:
            data = self.replace_column_with_id(data, *args)
        with ChiBased() as db:
            db.insert(
                "inspections",
                [
                    "id",
                    "license_number",
                    "facility_address_id",
                    "inspection_type_id",
                    "result_type_id",
                    "date",
                ],
                data.values.tolist(),
            )

    def parse_violation(self, violation: str) -> list[tuple[str, str, str]]:
        """Parse a violation entry into a list of violations.

        Each return list element will be the violation number, violation title, and comments.
        """
        violations = violation.replace("&", "And").split(" | ")
        parsed_violations = []
        for violation in violations:
            if "Comments" in violation:
                parsed_violation = re.findall(
                    r"([0-9]{1,2})\. (.+) - Comments: (.+)", violation
                )
                parsed_violations.append(
                    [
                        int(parsed_violation[0][0]),
                        parsed_violation[0][1],
                        parsed_violation[0][2],
                    ]
                )
            else:
                parsed_violation = re.findall(r"([0-9]{1,2})\. (.+)", violation)
                parsed_violations.append(
                    [int(parsed_violation[0][0]), parsed_violation[0][1], ""]
                )
        return parsed_violations

    @time_it()
    def insert_violations_data(self, data: pandas.DataFrame):
        """Populate `violations` and `violation_types` tables."""
        data = data[["violations", "inspection_id"]].dropna(subset=["violations"])
        with ChiBased() as db:
            inspection_ids = [
                list(row.values())[0] for row in db.select("inspections", ["id"])
            ]

        unique_violations = {}
        inspection_violations = []
        for item in data.values.tolist():
            violation, inspection_id = item
            for violation in self.parse_violation(violation):
                violation_id, violation, comment = violation
                if violation_id not in unique_violations:
                    unique_violations[violation_id] = violation
                if inspection_id in inspection_ids:
                    inspection_violations.append((inspection_id, violation_id, comment))
        unique_violations = [
            (key, unique_violations[key])
            for key in sorted(list(unique_violations.keys()))
        ]
        with ChiBased() as db:
            db.insert("violation_types", ["id", "name"], unique_violations)
            db.insert(
                "violations",
                ["inspection_id", "violation_type_id", "comment"],
                inspection_violations,
            )

    @time_it()
    def prepare_data(self) -> pandas.DataFrame:
        """Run preparation pipeline and return dataframe."""
        data = self.load()
        data = self.rename_columns(data)
        data = self.normalize_strings(data)
        data = self.fix_cities(data)
        data = self.remove_non_chicago_entries(data)
        data = self.remove_schools(data)
        data = self.convert_dates(data)
        data = self.fill_missing(data)
        return data


@time_it()
def prune():
    """Prune unneeded data."""
    with ChiBased() as db:
        db.query(
            """ DELETE FROM licenses WHERE license_number NOT IN (SELECT license_number FROM inspected_businesses);"""
        )
        db.query(
            """ DELETE FROM inspected_businesses WHERE license_number NOT IN (SELECT license_number FROM licenses); """
        )
        db.query(
            """ DELETE FROM businesses WHERE account_number NOT IN (SELECT account_number FROM licenses); """
        )
        db.query(
            """ DELETE FROM business_addresses WHERE id NOT IN (SELECT address_id FROM businesses); """
        )
        db.query(
            """ DELETE FROM violations WHERE inspection_id NOT IN (SELECT id FROM inspections); """
        )
        db.close()
        db.vacuum()


@time_it()
def load_to_sqlite():
    (root / "chi.db").delete()
    with ChiBased() as db:
        db.create_tables_script()
    loader = BusinessLicenses()
    loader.load_data_to_db()
    loader = FoodInspections()
    loader.load_data_to_db()
    prune()


if __name__ == "__main__":
    load_to_sqlite()
