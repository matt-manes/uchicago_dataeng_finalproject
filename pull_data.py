import requests
from noiftimer import time_it
from pathier import Pathier

root = Pathier(__file__).parent


@time_it()
def download(url: str, filename: str) -> str | None:
    """Download content from `url` to `filename`.

    Raises an `Exception` if download fails."""
    response = requests.get(url)
    if response.status_code == 200:
        (root / filename).write_text(response.text, "utf-8")
    else:
        raise RuntimeError(
            f"Could not download dataset at {url}.\nStatus code: {response.status_code}"
        )


@time_it()
def pull():
    """Download most recent copy of datasets and save to local file."""
    datasets = {
        "business_licenses.csv": "https://data.cityofchicago.org/api/views/r5kz-chrr/rows.csv",
        "food_inspections.csv": "https://data.cityofchicago.org/api/views/qizy-d2wf/rows.csv",
    }
    for filename, url in datasets.items():
        print(f"Downloading {filename} from {url} ...")
        try:
            download(url, filename)
        except Exception as e:
            print(f"{type(e).__name__}: {e}")


if __name__ == "__main__":
    pull()
