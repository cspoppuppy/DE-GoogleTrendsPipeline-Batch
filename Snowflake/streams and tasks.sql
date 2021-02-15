use snowpipe;

//==============================================================================
// Create streams
//==============================================================================
create or replace stream stream_interest_over_time on table snowpipe.staging.interest_over_time;
create or replace stream stream_interest_by_region on table snowpipe.staging.interest_by_region;

show streams;

select * from stream_interest_over_time;
select * from stream_interest_by_region;

//==============================================================================
// Create tasks
//==============================================================================
// task for processing interest over time
create or replace task interest_over_time_processor
    warehouse = compute_wh
    schedule = 'USING CRON * * * * * America/Chicago'
when
    system$stream_has_data('stream_interest_over_time')
as
    // update dimension table
    merge into snowpipe.public.dim_searchterm t
    using
    (
        select distinct search_term
        from snowpipe.staging.interest_over_time st
        where st.search_term is not null
    ) s
    on t.search_term=s.search_term
    when not matched then insert (search_term) values (s.search_term);
    // insert records to fact table
    insert into snowpipe.public.fact_interest_over_time (date, search_term_id, value)
    select st.date, ds.id as search_term_id, st.value
    from stream_interest_over_time st
    left join snowpipe.public.dim_searchterm ds
    on st.search_term=ds.search_term
    where st.search_term is not null;
    
// task for processing interest by region
create or replace task interest_by_region_processor
    warehouse = compute_wh
    schedule = 'USING CRON * * * * * America/Chicago'
when
    system$stream_has_data('stream_interest_by_region')
as
    // update dimension table
    merge into snowpipe.public.dim_state t
    using
    (
        select distinct state
        from snowpipe.staging.interest_by_region st
        where st.state is not null
    ) s
    on t.state=s.state
    when not matched then insert (state) values (s.state);
    // insert records to fact table
    insert into snowpipe.public.fact_interest_by_region (date, state_id, search_term_id, value)
    select st.date, stt.id as state_id, ds.id as search_term_id, st.value
    from stream_interest_by_region st
    left join snowpipe.public.dim_searchterm ds
    on st.search_term=ds.search_term
    left join snowpipe.public.dim_state stt
    on st.state=stt.state
    where st.state is not null;
