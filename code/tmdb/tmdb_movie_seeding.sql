INSERT INTO link.tmdb_movie_details (tconst, tmdb_id, found_status)
(
    SELECT tconst, tmdb_id, 'pending' AS found_status
    FROM link.movies
    WHERE link_status = 'success'
)
ON CONFLICT (tmdb_id) DO NOTHING;
