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
