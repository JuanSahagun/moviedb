ALTER TABLE IF EXISTS intermediate.movie_crew
    ALTER COLUMN directors SET DATA TYPE text[]
        USING string_to_array(directors, ','),
    ALTER COLUMN writers SET DATA TYPE text[]
        USING string_to_array(writers, ',')
;
