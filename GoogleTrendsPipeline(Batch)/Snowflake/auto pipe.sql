//==============================================================================
// Create new database and schema
//==============================================================================
create database snowpipe;

create schema staging;

//==============================================================================
// Create csv file format
//==============================================================================
create or replace file format mycsvformat
  type = 'CSV'
  field_delimiter = ','
  skip_header = 1;

//==============================================================================
// Create stages connecting to S3 buckets
//==============================================================================
create or replace stage snowpipe.staging.googletrends_stage1
url='s3://lastgreedymosquito-googletrends1/'
credentials = (AWS_KEY_ID = '***********' AWS_SECRET_KEY = '***********')
file_format = mycsvformat;

create or replace stage snowpipe.staging.googletrends_stage2
url='s3://lastgreedymosquito-googletrends2/'
credentials = (AWS_KEY_ID = '***********' AWS_SECRET_KEY = '***********')
file_format = mycsvformat;

show stages;

//==============================================================================
// Create tables for CSV data
//==============================================================================
create or replace table snowpipe.staging.interest_over_time
(
    date datetime, 
    search_term varchar,
    value number
);

create or replace table snowpipe.staging.interest_by_region
(
    date datetime, 
    state varchar,
    search_term varchar,
    value number
);


//==============================================================================
// Create a pipe to ingest CSV data
//==============================================================================
create or replace pipe snowpipe.staging.googletrends_pipe1 auto_ingest=true as 
    copy into snowpipe.staging.interest_over_time
    from @snowpipe.staging.googletrends_stage1
    --file_format = (type = 'CSV');
    on_error = "skip_file";
    
create or replace pipe snowpipe.staging.googletrends_pipe2 auto_ingest=true as 
    copy into snowpipe.staging.interest_by_region
    from @snowpipe.staging.googletrends_stage2
    on_error = "skip_file";

show pipes;
