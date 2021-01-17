//==============================================================================
// Create read only role for snowpipe database
//==============================================================================
create role snowpipe_read_only
comment = 'This role is limited to querying tables in staging and public';

grant usage
on database snowpipe
to role snowpipe_read_only;

grant usage
on schema snowpipe.staging
to role snowpipe_read_only;

grant select
on all tables in schema snowpipe.staging
to role snowpipe_read_only;

grant usage
on schema snowpipe.public
to role snowpipe_read_only;

grant select
on all tables in schema snowpipe.public
to role snowpipe_read_only;

grant usage
on warehouse compute_wh
to role snowpipe_read_only;

//==============================================================================
// Create test_user
//==============================================================================
create user test_user password='**********' default_role = snowpipe_read_only must_change_password = false;
