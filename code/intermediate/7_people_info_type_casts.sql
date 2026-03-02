ALTER TABLE IF EXISTS intermediate.people_info
    ALTER COLUMN birthyear SET DATA TYPE integer
        USING birthyear::integer,
    ALTER COLUMN deathyear SET DATA TYPE integer
        USING deathyear::integer,
    ALTER COLUMN primaryprofession SET DATA TYPE text[]
        USING string_to_array(primaryprofession, ','),
    ALTER COLUMN knownfortitles SET DATA TYPE text[]
        USING string_to_array(knownfortitles, ',')
;
