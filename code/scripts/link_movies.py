import requests

import os
from dotenv import load_dotenv

import psycopg
from psycopg.types.json import Jsonb

# Max amount of movies to attempt to link.
movie_limit =  1000
# Batch size of each Linking & Writing iteration.
batch_size = 100

# * This query retrieves the IMDb movie ids that have a link status of 
#   'pending' or 'error' (with an attempt limit of 3).
# * Movies with higher vote counts are prioritized.
select_tconst_sql = f"""
SELECT tconst
FROM link.movies l
    INNER JOIN intermediate.rated_movies r
    ON l.tconst = r.tconst
WHERE l.link_status = 'pending'
    OR (l.link_status = 'error' AND l.attempt_count < 3)
ORDER BY r.numvotes DESC
LIMIT {movie_limit}
;
"""

# * This query will be used to update the status of a movie that was
#   just attempted to be linked to the TMDB database.
# * Replacement value order: (tmdb_id, link_status, result, last_error, tconst)
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
    
    # Add the IMDb ids to the result set
    cur.execute(select_tconst_sql)
    