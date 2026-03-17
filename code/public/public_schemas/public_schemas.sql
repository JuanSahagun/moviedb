CREATE TABLE IF NOT EXISTS public.movies (
    tconst text,
    tmdb_id int,
    primary_title text,
    release_year int,
    runtime int,
    budget bigint,
    revenue bigint,
    avg_rating numeric(3, 1),
    num_votes int,
    PRIMARY KEY (tconst),
    CONSTRAINT valid_year 
        CHECK (release_year >= 1888 AND release_year <= 9999),
    CHECK (runtime >= 0),
    CHECK (budget >= 0),
    CHECK (revenue >= 0),
    CHECK (avg_rating >= 0),
    CHECK (num_votes >= 0)
);

CREATE TABLE IF NOT EXISTS public.movie_genres (
    tconst text,
    genre text,
    PRIMARY KEY (tconst, genre),
    FOREIGN KEY (tconst) REFERENCES public.movies (tconst)
);

CREATE TABLE IF NOT EXISTS public.people (
    nconst text,
    primary_name text,
    birth_year int,
    death_year int,
    PRIMARY KEY (nconst),
    CONSTRAINT valid_birth_year 
        CHECK (birth_year >= 1111 AND birth_year <= 9999),
    CONSTRAINT valid_death_year 
        CHECK (death_year >= 1111 AND death_year <= 9999)
);

CREATE TABLE IF NOT EXISTS public.movie_roles (
    tconst text,
    nconst text,
    ordering int,
    category text,
    job text,
    characters text,
    PRIMARY KEY (tconst, nconst, ordering),
    FOREIGN KEY (tconst) REFERENCES public.movies (tconst)
);
