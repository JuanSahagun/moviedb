import requests

import os
from dotenv import load_dotenv

import psycopg
from psycopg.types.json import Jsonb

# * This query will be used to update the status of a movie that was
#   just attempted to be linked to the TMDB database.
# * Replacement value order: (tmdb_id, link_status, result, last_error, tconst)
update_sql = """
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
