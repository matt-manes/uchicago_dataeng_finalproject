from databased import DataBased
import pandas
from pathier import Pathier
from typing import Any
from datetime import datetime

root = Pathier(__file__).parent


class ExpoBased(DataBased):
    def __init__(self):
        super().__init__("expo.db")
        self.make_tables()

    def make_tables(self):
        self.create_business_licenses()
        self.create_food_inspections()

    def drop_tables(self):
        self.drop_table("business_licenses")
        self.drop_table("food_inspections")

    def insert_many(self, table: str, values: list[tuple[Any]]):
        n = 900
        for i, row in enumerate(values):
            values[i] = [
                item.date() if isinstance(item, pandas.Timestamp) else item
                for item in row
            ]
        for i in range(0, len(values), n):
            chunk = values[i : i + n]
            # input(chunk[0])
            """ sizes = {}
            for row in chunk:
                if len(row) < 17:
                    input(row[0])
                size = len(row)
                if size not in sizes:
                    sizes[size] = 1
                else:
                    sizes[size] += 1
            if len(sizes) > 1:
                print(sizes)
                print(f"{i}-{i+n}")
                input("...") """
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
                "application_created_date text",
                "application_requirements_complete text",
                "payment_date text",
                "conditional_approval text",
                "license_term_start_date text",
                "license_term_expiration_date text",
                "license_approved_for_issuance text",
                "date_issued text",
                "license_status text",
                "license_status_change_date text",
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
                "inspection_date text",
                "inspection_type text",
                "results text",
                "violations text",
                "latitude real",
                "longitude real",
                "location text",
            ],
        )


if __name__ == "__main__":
    with ExpoBased() as db:
        db.drop_tables()
        db.make_tables()

        data = pandas.read_csv("Business_Licenses.csv")
        for column in [
            "APPLICATION CREATED DATE",
            "APPLICATION REQUIREMENTS COMPLETE",
            "PAYMENT DATE",
            "LICENSE TERM START DATE",
            "LICENSE TERM EXPIRATION DATE",
            "LICENSE APPROVED FOR ISSUANCE",
            "DATE ISSUED",
            "LICENSE STATUS CHANGE DATE",
        ]:
            data[column] = data[column].fillna(
                (datetime.fromtimestamp(0)).strftime("%m/%d/%Y")
            )
            data[column] = pandas.to_datetime(data[column], format="%m/%d/%Y")

            # data[column] = data[column].astype(datetime)
            # data[column] = datetime.strptime(data[column], "%m/%d/%Y")
        # print(data)
        # input("...")
        data = data.fillna("0").values.tolist()
        # input(data[0])
        db.insert_many("business_licenses", data)

        data = pandas.read_csv("Food_Inspections.csv")
        data["Inspection Date"] = data["Inspection Date"].fillna(
            (datetime.fromtimestamp(0)).strftime("%m/%d/%Y")
        )
        data["Inspection Date"] = pandas.to_datetime(
            data["Inspection Date"], "%m/%d/%Y"
        )
        # data["Inspection Date"] = data["Inspection Date"].astype(datetime)
        data = data.fillna("0").values.tolist()
        db.insert_many("food_inspections", data)
