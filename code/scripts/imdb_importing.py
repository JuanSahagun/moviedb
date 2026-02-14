import psycopg
from psycopg import sql
import gzip
from pathlib import Path

# Path to the `moviedb/data/` directory
DATA_DIR = Path(__file__).parents[2] / "data"
TITLE_CREW_FILE = DATA_DIR / "title.crew.tsv.gz"

# COPY_SQL = """
# COPY staging.title_crew (tconst, directors, writers)
# FROM STDIN
# WITH (
#     FORMAT text,
#     DELIMITER E'\\t',
#     NULL '\\N'
# )
# """


# Key: file name
# Value: [table_name, column list]
DATASET_CONFIG = {
    "title.basics.tsv.gz": [
        "title_basics",
        ["tconst", "titleType", "primaryTitle", "originalTitle", "isAdult", "startYear", "endYear", "runtimeMinutes", "genres"],
    ],
    "title.crew.tsv.gz": [
        "title_crew",
        ["tconst", "directors", "writers"],
    ],
    "title.principals.tsv.gz": [
        "title_principals",
        ["tconst", "ordering", "nconst", "category", "job", "characters"],
    ],
    "title.ratings.tsv.gz": [
        "title_ratings",
        ["tconst", "averageRating", "numVotes"],
    ],
    "name.basics.tsv.gz": [
        "name_basics",
        ["nconst", "primaryName", "birthYear", "deathYear", "primaryProfession", "knownForTitles"],
    ],
}

TABLE_HAS_DATA_SQL = sql.SQL("""
SELECT EXISTS (
    SELECT 1
    FROM staging.{}
    LIMIT 1
)
""")

COPY_SQL = sql.SQL("""
    COPY staging.{} ({})
    FROM STDIN
    WITH (
        FORMAT text,
        DELIMITER E'\\t',
        NULL '\\N'
    )                   
""")

def load_imdb_dumps() -> None:
    # Connect to the database
    with psycopg.connect("postgresql://localhost/moviedb") as con:
        # Get a cursor to perform db operation
        with con.cursor() as cur:
            # Load each file
            for file_name, (table_name, col_names) in DATASET_CONFIG.items():
                # Check if the file has already been imported
                if already_loaded(con, table_name):
                    print(f"Skipping {file_name}: staging.{table_name} already has data")
                    continue

                # File has not yet been loaded
                


def already_loaded(con: psycopg.Connection, table_name: str) -> bool:
    with con.cursor() as cur:
        cur.execute(
            TABLE_HAS_DATA_SQL.format(sql.Identifier(table_name))
        )
        return cur.fetchone()[0]
        


# def load_title_crew() -> None:
#     if not TITLE_CREW_FILE.exists():
#         raise FileNotFoundError(f"Missing dataset file: {TITLE_CREW_FILE}")

#     # Connect to the db
#     with psycopg.connect("postgresql://localhost/moviedb") as con:
#         # Get a cursor to perform db operations
#         with con.cursor() as cur:
#             # Open the zip with the dataset
#             with gzip.open(TITLE_CREW_FILE, "rt", encoding="utf-8", newline="") as f:
#                 # Use a Copy object to execute the COPY FROM query for each line
#                 with cur.copy(COPY_SQL) as copy:
#                     # Skip the header line
#                     next(f, None)
#                     for line in f:
#                         copy.write(line)


# if __name__ == "__main__":
#     load_title_crew()
#     print(f"Loaded {TITLE_CREW_FILE.name} into staging.title_crew")
