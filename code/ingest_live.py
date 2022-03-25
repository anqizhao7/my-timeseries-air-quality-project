from numpy import float64, int32, string_
import requests
import json
import pandas as pd
from pandas import json_normalize
import datetime as dt
import config as c
# Import the new influxdb API client
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS


payload1 = {'Key': c.WEATHER_API_KEY, 'q': 'Kunming', 'aqi': 'yes'}
payload2 = {'Key': c.WEATHER_API_KEY, 'q': 'new york', 'aqi': 'yes'}
r1 = requests.get("http://api.weatherapi.com/v1/current.json", params=payload1)
r2 = requests.get("http://api.weatherapi.com/v1/current.json", params=payload2)

# get the json
r_string1 = r1.json()
r_string2 = r2.json()


# normalize the nested json
normalized1 = json_normalize(r_string1)
normalized2 = json_normalize(r_string2)




# you only get the localized time that's why timestamp format with +08.00 and -05.00 is very important (for kunming and new york) 
# otherwise TS will be in UTC and therefore in the future -> it will not get shown on the board
normalized1['TimeStamp'] = normalized1['location.localtime_epoch'].apply(lambda s : dt.datetime.fromtimestamp(s).strftime('%Y-%m-%dT%H:%M:%SZ'))
normalized2['TimeStamp'] = normalized2['location.localtime_epoch'].apply(lambda s : dt.datetime.fromtimestamp(s).strftime('%Y-%m-%dT%H:%M:%SZ'))

# concat two dfs
normalized = pd.concat([normalized1, normalized2], axis=0)

# rename the columns
normalized.rename(columns={'location.name': 'location', 
      'location.region': 'region',
      'current.temp_c': 'temp_c',
      'current.air_quality.co': 'co',
      'current.air_quality.no2': 'no2',
      'current.air_quality.o3': 'o3',
      'current.air_quality.so2': 'so2',
      'current.air_quality.pm2_5': 'pm25',
      'current.air_quality.pm10': 'pm10'
      }, inplace=True)     
print(normalized)
print(normalized.dtypes)

# set the index to the new timestamp
normalized.set_index('TimeStamp', inplace = True)

# filter out just columns we want to keep
df = normalized.filter(['temp_c','location','region', 'co','no2','o3','so2','pm25','pm10'])      

print(df)
# print(df.dtypes)

client = influxdb_client.InfluxDBClient(
   url = 'http://localhost:8086',
   token = c.INFLUXDB_TOKEN,
   org = 'my-org'
)

# define tag fields
datatags = ['region']

#write the test data into measurement
write_api = client.write_api(write_options=SYNCHRONOUS)
message = write_api.write(bucket='live_air_quality',org='my-org',record = df, data_frame_measurement_name = 'api', data_frame_tag_columns = datatags, debug = True)
write_api.flush()
print(message)