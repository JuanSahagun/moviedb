CREATE SCHEMA IF NOT EXISTS intermediate;

-- Contains the tconst values for movies that have been rated at least once
CREATE TABLE IF NOT EXISTS intermediate.rated_movie_tconsts AS (
    SELECT tb.tconst AS tconst
    FROM staging.title_basics tb INNER JOIN staging.title_ratings tr
    ON tb.tconst = tr.tconst
    WHERE tb."titleType" = 'movie'
);

-- Only retain attributes relevant to movies, and CAST to correct data types.
CREATE TABLE IF NOT EXISTS intermediate.movie_basics AS (
    SELECT
        "tconst" AS tconst,
        "primaryTitle" AS primarytitle,
        "originalTitle" AS originaltitle,
        CAST("isAdult" AS boolean) AS isadult,
        CAST("startYear" AS integer) AS startyear,
        CAST("runtimeMinutes" AS integer) AS runtimeminutes,
        CAST("genres" AS VARCHAR(32)) AS genres
    FROM staging.title_basics
    WHERE "tconst" IN (SELECT tconst FROM intermediate.rated_movie_tconsts)
);

CREATE TABLE IF NOT EXISTS intermediate.movie_crew AS (
    SELECT tconst, directors, writers
    FROM staging.title_crew
    WHERE tconst IN (SELECT tconst FROM intermediate.rated_movie_tconsts)
);

CREATE TABLE IF NOT EXISTS intermediate.movie_principals AS (
    SELECT
        tconst,
        CAST(ordering AS integer) AS ordering,
        nconst,
        category,
        job,
        characters
    FROM staging.title_principals
    WHERE tconst IN (SELECT tconst FROM intermediate.rated_movie_tconsts)
);
