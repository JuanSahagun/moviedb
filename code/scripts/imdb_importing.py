import psycopg
from psycopg import sql
import gzip
from pathlib import Path

# Path to the `data/` directory
DATA_DIR = Path(__file__).parents[2] / "data"
# TITLE_CREW_FILE = DATA_DIR / "title.crew.tsv.gz"

# Key: file name
# Value: [table_name, column name list]
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
        # Load each file
        for file_name, (table_name, col_names) in DATASET_CONFIG.items():
            # Check if the file has already been imported
            if already_loaded(con, table_name):
                print(f"Skipping {file_name}: staging.{table_name} already has data")
                continue

            # File has not yet been loaded
            try:
                load_file(con, file_name, table_name, col_names)
                con.commit()
                print(f"Loaded {file_name} into staging.{table_name}")
            except Exception as e:
                con.rollback()
                print(f"Failed loading {file_name} into staging.{table_name}: {e}")

    print("Finished loading process. Check for any failures.")


def already_loaded(con: psycopg.Connection, table_name: str) -> bool:
    with con.cursor() as cur:
        cur.execute(
            TABLE_HAS_DATA_SQL.format(sql.Identifier(table_name))
        )
        return cur.fetchone()[0]
    
def load_file(con: psycopg.Connection, file_name: str, table_name: str, col_names: list[str]) -> None:
    file_path = DATA_DIR / file_name

    # Create an Identifier for each col name
    column_identifiers = []
    for col in col_names:
        column_identifiers.append(sql.Identifier(col))
    # Merge the Identifiers into a single one
    column_list_sql = sql.SQL(", ").join(column_identifiers)

    # Now we can use that as a single identifier to create
    # the COPY FROM query
    copy_stmt = COPY_SQL.format(
        sql.Identifier(table_name),
        column_list_sql
    )

    # Load the dataset if present
    with con.cursor() as cur:
        if not file_path.exists():
            raise FileNotFoundError(f"Missing dataset file: {file_name}")
        
        with gzip.open(file_path, "rt", encoding="utf-8", newline="") as f:
            with cur.copy(copy_stmt) as copy:
                # Skip the header line, then write to table
                next(f, None)
                for line in f:
                    copy.write(line)

if __name__ == "__main__":
    load_imdb_dumps()
