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
    FORMAT csv,
    DELIMITER E'\\t',
    NULL '\\N',
    HEADER true
)
"""


def load_title_crew() -> None:
    if not TITLE_CREW_FILE.exists():
        raise FileNotFoundError(f"Missing dataset file: {TITLE_CREW_FILE}")

    with psycopg.connect("postgresql://localhost/moviedb") as con:
        with con.cursor() as cur:
            with gzip.open(TITLE_CREW_FILE, "rt", encoding="utf-8", newline="") as f:
                with cur.copy(COPY_SQL) as copy:
                    for line in f:
                        copy.write(line)


if __name__ == "__main__":
    load_title_crew()
    print(f"Loaded {TITLE_CREW_FILE.name} into staging.title_crew")
