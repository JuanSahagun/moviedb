from requests_ratelimiter import LimiterSession

import os
from dotenv import load_dotenv

import psycopg
from psycopg.types.json import Jsonb

# Load API key from the enviornment variable
load_dotenv()
api_key = os.getenv('TMDB_API_KEY')

# Max amount of movies to attempt to find.
movie_limit =  100
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
    OR (found_status = 'error' AND attempt_count < 30)
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
    attempt_count = attempt_count + 1,
    last_error = %s,
    last_attempt = NOW()
WHERE tmdb_id = %s
;
"""

def find_movies() -> None:
    # Prevent rate-limiting
    session = LimiterSession(per_second=20)

    # Connect to the db
    with psycopg.connect("postgresql://localhost/moviedb") as conn:
        with conn.cursor() as read_cur:
            with conn.cursor() as write_cur:
                # Add the IMDB ID's to the result set
                read_cur.execute(select_tmdb_sql)

                batch_num = 0
                total = 0

                # Find and update one batch at a time
                while next_batch := read_cur.fetchmany(batch_size):
                    batch_num += 1
                    total += len(next_batch)
                    # Flatten the list of 1-tuples into a list of integers.
                    next_batch = [tup[0] for tup in next_batch]

                    # Obtain the results
                    batch_details = get_details(session, next_batch)
                    # Write the results
                    write_updates(write_cur, batch_details)
                    # Commit pending transactions and display progress
                    conn.commit()
                    print(f"Batch {batch_num} processed. ({total/movie_limit*100:.2f}% complete)")
                print("Job completed.")



def get_details(session: LimiterSession, movie_ids: list[int]) -> list[tuple]:
    # A list of tuples, for the values to be used in the UPDATE statement
    update_tups = []

    # Set the headers and query parameters for the request
    headers = {
        'accept': 'application/json',
        'authorization': f'Bearer {api_key}'
    }
    params = {
        'external_source': 'imdb_id',
        'language': 'en-US'
    }

    # Call the Movie Details endpoint for each movie.
    # On each iteration, append a 4-tuple with the UPDATE statement's
    # replacement values.
    for id in movie_ids:
        url = f"https://api.themoviedb.org/3/movie/{id}"

        try:
            resp = session.get(url, headers=headers, params=params)
            resp.raise_for_status()
            data = resp.json()
            update_vals = get_replacement_vals(id, data)
            update_tups.append(update_vals)
        except Exception as e:
            update_tups.append(
                ('error', None, str(e), id)
            )

    return update_tups



def get_replacement_vals(tmdb_id, data) -> tuple[str, Jsonb, str, int]:
    """
    (found_status, result, last_error, tmdb_id)
    """
    fields = data.keys()

    # Edgecase, movie ID was not recognized.
    if "success" in fields and data["success"] is False:
        return ('not_found', Jsonb(data), data["status_message"], tmdb_id)
    
    return ('success', Jsonb(data), None, tmdb_id)


def write_updates(cur: psycopg.Cursor, updates: list[tuple]) -> None:
    cur.executemany(update_status_sql, updates)

if __name__ == '__main__':
    find_movies()
