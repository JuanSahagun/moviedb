-- Cast numeric columns to the correct data types,
-- and convert genres into an array.
ALTER TABLE IF EXISTS intermediate.movie_basics
    ALTER COLUMN isadult SET DATA TYPE boolean
        USING isadult::boolean,
    ALTER COLUMN startyear SET DATA TYPE integer
        USING startyear::integer,
    ALTER COLUMN runtimeminutes SET DATA TYPE integer
        USING runtimeminutes::integer,
    ALTER COLUMN genres SET DATA TYPE text[]
        USING string_to_array(genres, ',')
;

