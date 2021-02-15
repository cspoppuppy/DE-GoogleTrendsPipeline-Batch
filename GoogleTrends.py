from pytrends.request import TrendReq
from datetime import datetime
from io import StringIO
import boto3
import pandas as pd

def LogToCSV(msg):
	with open('log.csv', mode='a') as f:
		f.write("{},{}".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S"),msg))
		f.write("\n")

try:
    # Get data from Google Trends to DataFrames
    pytrends = TrendReq(hl='en-US', tz=360)
    kw_list = ["Elon Musk", "Joe Biden", "Taylor Swift", "Godzilla", "Black Panther"]
    pytrends.build_payload(kw_list, cat=0, timeframe='now 1-H', geo='US', gprop='')
    df_interest_over_time_raw = pytrends.interest_over_time()
    df_Interest_by_region_raw = pytrends.interest_by_region()

    # save raw data from DataFrames as csv in S3 bucket1
    s3_resource = boto3.resource('s3')
    dt=datetime.now()
    timestr = dt.strftime("%Y%m%d%H%M%S") # timestamp append to csv file name
    raw_bucket = 'lastgreedymosquito-bucket1'
    # interest_over_time
    csv_buffer = StringIO()
    df_interest_over_time_raw.to_csv(csv_buffer)
    s3_resource.Object(raw_bucket, "mutiTimeline{}.csv".format(timestr)).put(Body=csv_buffer.getvalue())
    # interest_by_region
    csv_buffer = StringIO()
    df_Interest_by_region_raw.to_csv(csv_buffer)
    s3_resource.Object(raw_bucket, "geoMap{}.csv".format(timestr)).put(Body=csv_buffer.getvalue())

    # Manipulate data
    # interest over time
    # stack the search terms
    df_interest_over_time_raw.reset_index(inplace=True)#.set_index("date")
    df_interest_over_time_raw.set_index("date", inplace=True)
    df_interest_over_time=pd.DataFrame(df_interest_over_time_raw.stack()).reset_index()
    # rename to sensible column names
    df_interest_over_time.rename(columns={"level_1":"search_term", 0:"value"}, inplace=True)
    # order by search_term, date
    df_interest_over_time.sort_values(by=['search_term','date'], inplace=True)
    df_interest_over_time=df_interest_over_time[df_interest_over_time.search_term!='isPartial']
    # interest by region
    # add extra date column
    dt_truncated=dt.replace(minute=0, second=0, microsecond=0) # precision up to hour
    df_Interest_by_region_raw.reset_index(inplace=True)
    df_Interest_by_region_raw['date']=dt_truncated
    df_Interest_by_region_raw.set_index(["date",'geoName'], inplace=True)
    df_Interest_by_region=pd.DataFrame(df_Interest_by_region_raw.stack()).reset_index()
    df_Interest_by_region.rename(columns={'level_2':'search_term', 0:'value'}, inplace = True)
    # order by search_term, date, geoName
    df_Interest_by_region.sort_values(by=['search_term','date','geoName'], inplace=True)

    # save processed data as csv in S3 googletrends1 and googletrends2 respectively
    bucket1 = 'lastgreedymosquito-googletrends1'
    bucket2 = 'lastgreedymosquito-googletrends2'
    # interest_over_time
    csv_buffer = StringIO()
    df_interest_over_time.to_csv(csv_buffer, index=False)
    s3_resource.Object(bucket1, "overTime{}.csv".format(timestr)).put(Body=csv_buffer.getvalue())
    # interest_by_region
    csv_buffer = StringIO()
    df_Interest_by_region.to_csv(csv_buffer, index=False)
    s3_resource.Object(bucket2, "byRegion{}.csv".format(timestr)).put(Body=csv_buffer.getvalue())
    LogToCSV("Successfully processed batch")
except Exception as e:
    LogToCSV("Failed to process batch: " + str(e))
