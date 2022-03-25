import pandas as pd
import influxdb_client
from influxdb_client.client.write_api import SYNCHRONOUS
import influxdb_client.client.influxdb_client
import config as c


client = influxdb_client.InfluxDBClient(
   url = 'http://localhost:8086',
   token = c.INFLUXDB_TOKEN,
   org = 'my-org'
)

queryAPI = client.query_api()

#create flux query
myquery = 'from(bucket: "air-quality") |> range(start: 2014-01-01T00:00:00Z, stop: 2022-02-28T00:00:00Z)' \
            '|> filter(fn: (r) => r["_measurement"] == "historical-air-quality")' \
            '|> filter(fn: (r) => r["_field"] == "pm25")' 

df = queryAPI.query_data_frame( query = myquery)

print(df.info())
print(df)

