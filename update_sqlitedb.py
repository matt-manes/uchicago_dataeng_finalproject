import pull_data
import chibased
from printbuddies import print_in_place
from noiftimer import Timer
from concurrent.futures import ProcessPoolExecutor
import time


def update():
    pull_data.pull()
    chibased.load_data_to_db()


def main():
    """Pull current datasets and load into database."""
    timer = Timer().start()
    with ProcessPoolExecutor() as executor:
        process = executor.submit(update)
        while not process.done():
            print_in_place(
                f"Updating sqlite database... {timer.elapsed_str}", animate=True
            )
            time.sleep(1)
    print()
    print("Update complete.")


if __name__ == "__main__":
    main()
