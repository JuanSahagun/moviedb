INSERT INTO public.people (nconst, primary_name, birth_year, death_year)
(
    WITH linked_movies AS (
        SELECT m.tconst
        FROM intermediate.movie_basics m
            INNER JOIN link.tmdb_movie_details t
            ON m.tconst = t.tconst
            WHERE t.found_status = 'success'
    )
    SELECT i.nconst, i.primaryname,
        CASE WHEN i.birthyear >= 1800 AND i.birthyear <= 9999 THEN i.birthyear ELSE NULL END,
        CASE WHEN i.deathyear >= 1800 AND i.deathyear <= 9999 THEN i.deathyear ELSE NULL END
    FROM intermediate.people_info i
    WHERE nconst IN (
        SELECT DISTINCT(nconst)
        FROM intermediate.movie_principals p
        WHERE p.tconst IN (SELECT tconst FROM linked_movies)
    )
);
