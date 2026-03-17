-- The most profitable films by ROI, 
-- with a minimum of 50k votes.

SELECT
    m.primary_title,
    m.release_year,
    m.budget,
    m.revenue,
    ROUND(m.revenue::numeric / m.budget, 1) AS roi_ratio,
    m.avg_rating
FROM movies m
WHERE m.budget > 0 AND m.revenue > 0
    AND m.num_votes >= 50000
ORDER BY roi_ratio DESC
LIMIT 20;