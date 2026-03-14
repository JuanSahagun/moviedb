from requests_ratelimiter import LimiterSession

import os
from dotenv import load_dotenv

import psycopg
from psycopg.types.json import Jsonb

# Load API key from the enviornment variable
load_dotenv()
api_key = os.getenv('TMDB_API_KEY')
