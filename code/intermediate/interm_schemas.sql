CREATE SCHEMA IF NOT EXISTS intermediate;

-- Delete tables that are no longer needed
DROP TABLE IF EXISTS intermediate.movie_tconsts;
DROP TABLE IF EXISTS intermediate.movie_ratings;
DROP TABLE IF EXISTS intermediate.movie_ratings_stats;

-- Contains the tconst values for movies that have been rated at least once
CREATE TABLE IF NOT EXISTS intermediate.rated_movie_tconsts AS (
    SELECT tb.tconst AS tconst
    FROM staging.title_basics tb INNER JOIN staging.title_ratings tr
    ON tb.tconst = tr.tconst
    WHERE tb."titleType" = 'movie'
);
