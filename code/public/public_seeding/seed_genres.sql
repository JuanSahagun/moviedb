INSERT INTO public.movie_genres (tconst, genre)
(
    WITH linked_movies AS (
        SELECT m.tconst, m.genres
        FROM intermediate.movie_basics m
            INNER JOIN link.tmdb_movie_details t
            ON m.tconst = t.tconst
            WHERE t.found_status = 'success'
    )
    SELECT tconst, unnest AS genre
    FROM linked_movies l, UNNEST(genres)
);
