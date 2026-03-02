ALTER TABLE IF EXISTS intermediate.movie_ratings
    ALTER COLUMN averagerating SET DATA TYPE numeric(3,1)
        USING averagerating::numeric(3,1),
    ALTER COLUMN numvotes SET DATA TYPE integer
        USING numvotes::integer
;
