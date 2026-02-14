import psycopg
import gzip
from pathlib import Path

# Path to the `moviedb/data/` directory
DATA_DIR = Path(__file__).parents[2] / "data"
TITLE_CREW_FILE = DATA_DIR / "title.crew.tsv.gz"

COPY_SQL = """
COPY staging.title_crew (tconst, directors, writers)
FROM STDIN
WITH (
    FORMAT text,
    DELIMITER E'\\t',
    NULL '\\N'
)
"""


def load_title_crew() -> None:
    if not TITLE_CREW_FILE.exists():
        raise FileNotFoundError(f"Missing dataset file: {TITLE_CREW_FILE}")

    # Connect to the db
    with psycopg.connect("postgresql://localhost/moviedb") as con:
        # Get a cursor to perform db operations
        with con.cursor() as cur:
            # Open the zip with the dataset
            with gzip.open(TITLE_CREW_FILE, "rt", encoding="utf-8", newline="") as f:
                # Use a Copy object to execute the COPY FROM query for each line
                with cur.copy(COPY_SQL) as copy:
                    # Skip the header line
                    next(f, None)
                    for line in f:
                        copy.write(line)


if __name__ == "__main__":
    load_title_crew()
    print(f"Loaded {TITLE_CREW_FILE.name} into staging.title_crew")
