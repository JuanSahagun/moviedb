import requests

import os
from dotenv import load_dotenv

import psycopg
from psycopg.types.json import Jsonb

# Max amount of movies to attempt to link.
movie_limit =  1000
# Batch size of each Linking & Writing iteration.
batch_size = 100

# * This query retrieves the IMDb movies that have a link_status of 
#   'pending' or 'error' (attempt limit < 3).
# * Movies with higher vote counts are prioritized.
select_tconst_sql = f"""
SELECT tconst
FROM link.movies l
    INNER JOIN intermediate.rated_movies r
    ON l.tconst = r.tconst
WHERE link_status = 'pending'
    OR (link_status = 'error' AND attempt_count < 3)
ORDER BY numvotes DESC
LIMIT {movie_limit}
;
"""

# * This query will be used to update the status of a movie that was
#   just attempted to be linked with the TMDB database.
# * Order of replacement values: (tmdb_id, link_status, result, last_error, tconst)
update_status_sql = """
UPDATE link.movies
SET
    tmdb_id = %s,
    link_status = %s,
    result = %s,
    attempt_count = attempt_count + 1,
    last_error = %s,
    last_attempt = NOW()
WHERE tconst = %s
;
"""


def link() -> None:
    # Connect to the db
    con = psycopg.connect("postgresql://localhost/moviedb")
    cur = con.cursor()
    
    # Add the IMDb movies to the result set
    cur.execute(select_tconst_sql)

    # Match & Update one batch at a time
    while batch := cur.fetchmany(batch_size):
        matches = get_matches( [t[0] for t in batch] )
        write_matches(matches)
    

def get_matches(tconsts: list[str]) -> list[tuple[int, str, Jsonb, str]]:
    # TODO: Write this function
    return None # Placeholder

def write_matches(updates: list[tuple[int, str, Jsonb, str]]) -> None:
    # TODO: Write this function
    return None # Placeholder
