//==============================================================================
// Create tables
//==============================================================================
create or replace table snowpipe.public.dim_searchterm
(
    id int identity(1,1) primary key,
    search_term varchar
);

create or replace table snowpipe.public.dim_state
(
    id int identity(1,1) primary key,
    state varchar
);

create or replace table snowpipe.public.fact_interest_over_time
(
    id int identity(1,1) primary key,
    date datetime, 
    search_term_id int references snowpipe.public.dim_searchterm(id),
    value number
);

create or replace table snowpipe.public.fact_interest_by_region
(
    id int identity(1,1) primary key,
    date datetime, 
    state_id int references snowpipe.public.dim_state(id),
    search_term_id int references snowpipe.public.dim_searchterm(id),
    value number
);
