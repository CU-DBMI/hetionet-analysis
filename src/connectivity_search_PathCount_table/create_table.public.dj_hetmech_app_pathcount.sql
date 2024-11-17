CREATE TABLE public.dj_hetmech_app_pathcount (
    id integer NOT NULL,
    path_count integer NOT NULL,
    dwpc double precision NOT NULL,
    p_value double precision,
    metapath_id character varying(20) NOT NULL,
    source_id integer NOT NULL,
    target_id integer NOT NULL,
    dgp_id integer NOT NULL,
    CONSTRAINT dj_hetmech_app_pathcount_path_count_check CHECK ((path_count >= 0))
);
