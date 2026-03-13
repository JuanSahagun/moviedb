CREATE SCHEMA IF NOT EXISTS link;

CREATE TABLE IF NOT EXISTS link.movies (
    tconst text PRIMARY KEY,
    tmdb_id integer,
    link_status text
        CHECK (link_status IN ('pending', 'success', 'ambiguous', 'error', 'not_found')),
    result jsonb,
    attempt_count integer NOT NULL DEFAULT 0,
    last_error text,
    last_attempt timestamp with time zone
);

CREATE TABLE IF NOT EXISTS link.tmdb_movie_details (
    tconst text,
    tmdb_id integer,
    found_status text,
    result jsonb,
    attempt_count integer NOT NULL DEFAULT 0,
    last_error text,
    last_attempt timestamp with time zone,

    PRIMARY KEY (tmdb_id),
    FOREIGN KEY (tconst) REFERENCES link.movies(tconst),
    CHECK (found_status IN ('pending', 'success', 'ambiguous', 'error', 'not_found'))
);
