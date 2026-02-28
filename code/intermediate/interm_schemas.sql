CREATE SCHEMA IF NOT EXISTS intermediate;

CREATE TABLE IF NOT EXISTS intermediate.movie_tconsts AS (
    SELECT "tconst" AS tconst
    FROM staging.title_basics
    WHERE "titleType" = 'movie'
);

CREATE TABLE IF NOT EXISTS intermediate.movie_ratings AS (
    SELECT "tconst" AS tconst,
    CAST("averageRating" AS double precision) AS averagerating,
    CAST("numVotes" AS integer) AS numvotes
    FROM staging.title_ratings
    WHERE "tconst" IN (SELECT tconst FROM intermediate.movie_tconsts)
);

CREATE TABLE IF NOT EXISTS intermediate.movie_ratings_stats AS (
    SELECT
    MIN(numvotes) AS minvotes,
    MAX(numvotes) AS maxvotes,
    AVG(numvotes) AS avgvotes,
    mode() WITHIN GROUP (ORDER BY numvotes ASC) AS votesmode,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY numvotes ASC)
        AS p25votes,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY numvotes ASC)
    AS p50votes,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY numvotes ASC)
        AS p75votes
    FROM intermediate.movie_ratings
);
