import pandas as pd
from numpy import float64
import datetime as dt
import config as c

# Import the new influxdb API client
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS

# import air quality data
df1 = pd.read_csv("data/kunming-air-quality.csv")
df2 = pd.read_csv("data/new-york-air-quality.csv")

# add cityName 
df1['cityName'] = 'Kunming'
df2['cityName'] = 'NewYork'

# print(df1.columns)
# print(df2.columns)
# print(len(df1))
# print(len(df2))

# concat two dfs
df = pd.concat([df1, df2], axis=0)
# print(df.columns)
# print(len(df))

# create a timestamp needed for influx 2020-01-01T00:00:00.00Z
df['TimeStamp'] = df['date'].apply(lambda s : pd.Timestamp(s))

# set the timestamp as the index of the dataframe
df.set_index('TimeStamp', inplace = True)

# drop the date field
df = df.drop(['date'], axis=1)



# change the column types to float
df = df.astype({"pm25": 'float',
               "pm10": 'float',
               "o3": 'float',
               "no2": 'float',
               "so2": 'float',
               "co": 'float',
               "cityName": 'str'
               })

#print(df.tail())

# define tag fields
datatags = ['cityName']

client = influxdb_client.InfluxDBClient(
   url = 'http://localhost:8086',
   token = c.INFLUXDB_TOKEN,
   org = 'my-org'
)

# write the data to bucket in influxdb
write_api = client.write_api(write_options = SYNCHRONOUS)
message = write_api.write(bucket = 'air-quality',org = 'my-org',record = df, data_frame_measurement_name = 'historical-air-quality', data_frame_tag_columns = datatags, debug = True)
print(message)

write_api.flush()





