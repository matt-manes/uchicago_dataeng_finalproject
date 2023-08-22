from pymongo import MongoClient
import pandas
from pathier import Pathier

root = Pathier(__file__).parent


def load_csv(file: Pathier) -> int:
    """Returns number of inserted records."""
    collection_name = file.stem.lower()
    db = MongoClient().chicago.get_collection(collection_name)
    db.delete_many({})
    data = pandas.read_csv(file).to_dict("records")
    return len(db.insert_many(data).inserted_ids)


if __name__ == "__main__":
    mongo = MongoClient()
    for file in root.glob("*.csv"):
        print(f"Loading data from {file.name}...")
        print(f"{load_csv(file)} records inserted.")
