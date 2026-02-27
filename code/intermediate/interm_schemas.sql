CREATE SCHEMA IF NOT EXISTS intermediate;

CREATE TABLE IF NOT EXISTS intermediate.movie_tconsts AS (
    SELECT "tconst" AS tconst
    FROM staging.title_basics
    WHERE "titleType" = 'movie'
);

DROP TABLE IF EXISTS intermediate.title_ratings;
CREATE TABLE IF NOT EXISTS intermediate.title_ratings AS (
    SELECT "tconst" AS tconst,
    CAST("averageRating" AS double precision) AS averagerating,
    CAST("numVotes" AS integer) AS numvotes
    FROM staging.title_ratings
    WHERE "tconst" IN (SELECT tconst FROM intermediate.movie_tconsts)
);

