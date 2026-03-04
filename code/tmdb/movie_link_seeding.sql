-- Seed link.movies with ALL rated-movie tconsts
INSERT INTO link.movies (tconst, link_status)
    (
        SELECT tconst, 'pending' AS link_status
        FROM intermediate.rated_movie_tconsts
    )
ON CONFLICT (tconst) DO NOTHING;
