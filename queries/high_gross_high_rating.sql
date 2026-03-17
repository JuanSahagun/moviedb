-- Films that were critically acclaimed, but also
-- had a high gross.

SELECT
    m.primary_title,
    m.release_year,
    m.revenue,
    m.budget,
    m.avg_rating,
    m.num_votes
FROM movies m
WHERE m.revenue > 500000000
    AND m.avg_rating >= 8.0
    AND m.num_votes >= 50000
ORDER BY m.revenue DESC
LIMIT 15;
