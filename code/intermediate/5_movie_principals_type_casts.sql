ALTER TABLE IF EXISTS intermediate.movie_principals
    ALTER COLUMN ordering SET DATA TYPE integer
        USING ordering::integer
;
