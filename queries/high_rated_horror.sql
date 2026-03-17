-- 15 Highest rated horror films of the 2010s with a significant audience

SELECT m.primary_title, m.release_year, m.avg_rating, m.num_votes
FROM movies m
    INNER JOIN movie_genres g ON m.tconst = g.tconst
WHERE g.genre = 'Horror'
    AND m.release_year BETWEEN 2010 AND 2019
    AND m.num_votes >= 50000
ORDER BY m.avg_rating DESC
LIMIT 15;
