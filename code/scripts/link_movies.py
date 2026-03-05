from requests_ratelimiter import LimiterSession

import os
from dotenv import load_dotenv

import psycopg
from psycopg.types.json import Jsonb

# Load API key from the enviornment variable
load_dotenv()
api_key = os.getenv('TMDB_API_KEY')


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
    # Prevent rate-limiting
    session = LimiterSession(per_second=20)

    # Connect to the db
    with psycopg.connect("postgresql://localhost/moviedb") as conn:
        with conn.cursor() as cur:

            # Add the IMDb movies to the result set
            cur.execute(select_tconst_sql)

            # Match & Update one batch at a time
            while batch := cur.fetchmany(batch_size):
                matches = get_matches(session, [t[0] for t in batch] )
                # TODO: Call the write_matches function
    

def get_matches(session: LimiterSession, tconsts: list[str]) -> list[tuple[int|None, str, Jsonb, str|None, str]]:
    # The update-values for this batch
    upd_lst = []

    # Set the headers and query parameters for the request
    headers = {
        'accept': 'application/json',
        'authorization': f'Bearer {api_key}'
    }
    parameters = {
        'external_source': 'imdb_id',
        'language': 'en-US'
    }

    # Call the Find BY ID endpoint for each movie
    for t in tconsts:
        url = f"https://api.themoviedb.org/3/find/{t}"
        try:
            resp = session.get(url, headers=headers, params=parameters)
            resp.raise_for_status()
            data = resp.json()

            # Append the tuple with the replacement values to this list
            upd_lst.append(get_repl_tup(t, data))

        except Exception as e:
            upd_lst.append(
                (None, 'error', None, str(e), t)
            )
    
    return upd_lst


def get_repl_tup(imdb_id, resp_data) -> tuple[int|None, str, Jsonb, str|None, str]:
    """
    If the request was successful, this function will be passed the response
    and return the 5-tuple of values to use when updating the link.movies table

    Order of replacement values: (tmdb_id, link_status, result, last_error, tconst)

    Note: If we are in this function, then last_error will always be None
    """
    tmdb_id = None
    link_status = ''
    result = Jsonb(resp_data)

    # Error handling if 'movie_results' key does not exist
    try:
        movie_lst = resp_data['movie_results']

        num_matches = len(movie_lst)
        if num_matches == 0:
            link_status = 'not_found'
        elif num_matches == 1:
            link_status = 'success'
            tmdb_id = movie_lst[0]['id']
        else:
            link_status = 'ambiguous'
        return (tmdb_id, link_status, result, None, imdb_id)
    except KeyError:
        link_status = 'error'
        last_error = 'Key Error. Did not find movie_results field.'
        return (tmdb_id, link_status, result, last_error, imdb_id)



def write_matches(updates: list[tuple[int|None, str, Jsonb, str|None, str]]) -> None:
    # TODO: Write this function.

    return None # Placeholder
