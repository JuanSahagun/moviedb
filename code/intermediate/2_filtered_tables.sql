DROP TABLE IF EXISTS intermediate.movie_basics;
CREATE TABLE intermediate.movie_basics AS (
    SELECT
        "tconst" AS tconst,
        "primaryTitle" AS primarytitle,
        "originalTitle" AS originaltitle,
        "isAdult" AS isadult,
        "startYear" AS startyear,
        "runtimeMinutes" AS runtimeminutes,
        "genres" AS genres
    FROM staging.title_basics
    WHERE "tconst" IN (SELECT tconst FROM intermediate.rated_movie_tconsts)
);

DROP TABLE IF EXISTS intermediate.movie_crew;
CREATE TABLE intermediate.movie_crew AS (
    SELECT tconst, directors, writers
    FROM staging.title_crew
    WHERE tconst IN (SELECT tconst FROM intermediate.rated_movie_tconsts)
);

DROP TABLE IF EXISTS intermediate.movie_principals;
CREATE TABLE intermediate.movie_principals AS (
    SELECT
        tconst,
        ordering,
        nconst,
        category,
        job,
        characters
    FROM staging.title_principals
    WHERE tconst IN (SELECT tconst FROM intermediate.rated_movie_tconsts)
);

DROP TABLE IF EXISTS intermediate.movie_ratings;
CREATE TABLE intermediate.movie_ratings AS (
    SELECT
        "tconst" AS tconst,
        "averageRating" AS averagerating,
        "numVotes" AS numVotes
    FROM staging.title_ratings
    WHERE tconst IN (SELECT tconst FROM intermediate.rated_movie_tconsts)
);

DROP TABLE IF EXISTS intermediate.people_info;
CREATE TABLE intermediate.people_info AS (
    SELECT
        "nconst" AS nconst,
        "primaryName" AS primaryname,
        "birthYear" AS birthyear,
        "deathYear" AS deathyear,
        "primaryProfession" AS primaryprofession,
        "knownForTitles" AS knownfortitles
    FROM staging.name_basics
    WHERE nconst IN (SELECT nconst FROM intermediate.rated_movie_nconsts)
);

