CREATE TABLE public.dj_hetmech_app_node (
    id integer NOT NULL,
    identifier character varying(50) NOT NULL,
    identifier_type character varying(50) NOT NULL,
    name character varying(200) NOT NULL,
    data jsonb NOT NULL,
    metanode_id character varying(50) NOT NULL
);
