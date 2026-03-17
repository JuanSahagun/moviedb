-- Films that lost the most money

SELECT
    m.primary_title,
    m.release_year,
    m.budget,
    m.revenue,
    m.revenue - m.budget AS profit,
    m.avg_rating
FROM movies m
WHERE m.budget > 0 AND m.revenue > 0
    AND m.num_votes >= 10000
ORDER BY profit ASC
LIMIT 20;
