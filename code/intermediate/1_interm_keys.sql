CREATE SCHEMA IF NOT EXISTS intermediate;

DROP TABLE IF EXISTS intermediate.rated_movie_tconsts;
CREATE TABLE intermediate.rated_movie_tconsts AS (
    SELECT tb.tconst AS tconst
    FROM staging.title_basics tb INNER JOIN staging.title_ratings tr
    ON tb.tconst = tr.tconst
    WHERE tb."titleType" = 'movie'
);

DROP TABLE IF EXISTS intermediate.rated_movie_nconsts;
CREATE TABLE intermediate.rated_movie_nconsts AS (
    SELECT DISTINCT(nconst)
    FROM staging.title_principals
    WHERE tconst IN (SELECT tconst FROM intermediate.rated_movie_tconsts)
);
