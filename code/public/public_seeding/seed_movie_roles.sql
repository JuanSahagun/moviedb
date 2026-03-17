INSERT INTO public.movie_roles (
        tconst, nconst, ordering,
        category, job, characters)
(
    WITH linked_movies AS (
        SELECT m.tconst
        FROM intermediate.movie_basics m
            INNER JOIN link.tmdb_movie_details t
            ON m.tconst = t.tconst
            WHERE t.found_status = 'success'
    )
    SELECT mp.tconst, mp.nconst, mp.ordering, mp.category,
        mp.job, mp.characters
    FROM intermediate.movie_principals mp
    WHERE mp.tconst IN (SELECT tconst FROM linked_movies)
);
