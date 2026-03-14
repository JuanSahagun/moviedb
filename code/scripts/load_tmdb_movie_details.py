from requests_ratelimiter import LimiterSession

import os
from dotenv import load_dotenv

import psycopg
from psycopg.types.json import Jsonb

# Load API key from the enviornment variable
load_dotenv()
api_key = os.getenv('TMDB_API_KEY')

# Max amount of movies to attempt to find.
movie_limit =  10000
# Batch size of each Linking & Writing iteration.
batch_size = 100

# * The query that will grab the TMDB IDs it will pass
#   to the Movie Details endpoint.                     
# * Movies with higher vote counts are prioritized.    
select_tmdb_sql = f"""
SELECT tmdb_id
FROM link.tmdb_movie_details t
    INNER JOIN intermediate.movie_ratings r
    ON t.tconst = r.tconst
WHERE found_status = 'pending'
    OR (found_status = 'error' AND attempt_count < 3)
ORDER BY numvotes DESC
LIMIT {movie_limit}
;
"""

# This query is used to update the status of the movies
# passed to the API, and store the returned JSON value.
update_status_sql = """
UPDATE link.tmdb_movie_details
SET
    found_status = %s,
    result = %s,
    attempt_count = attempt_count + 1
    last_error = %s,
    last_attempt = NOW()
WHERE tmdb_id = %s
;
"""
