INSERT INTO public.movies (tconst, tmdb_id, primary_title, release_year,
    runtime, budget, revenue, avg_rating, num_votes)
(
    SELECT m.tconst, t.tmdb_id, m.primarytitle, m.startyear, m.runtimeminutes,
        (t.result -> 'budget')::bigint,
        (t.result -> 'revenue')::bigint,
        r.averagerating,
        r.numvotes
    FROM intermediate.movie_basics m 
        INNER JOIN link.tmdb_movie_details t
        ON m.tconst = t.tconst
        INNER JOIN intermediate.movie_ratings r
        ON m.tconst = r.tconst
    WHERE found_status = 'success'
);
