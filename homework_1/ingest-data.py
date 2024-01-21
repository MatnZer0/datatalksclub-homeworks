import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

engine.connect()

# df = pd.read_csv('yellow_tripdata_2021-01.csv')
# df = pd.read_csv('green_tripdata_2019-09.csv')
df = pd.read_csv('taxi+_zone_lookup.csv')
# df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
# df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

df.to_sql(name='taxi_zone_lookup', con=engine, index=False, if_exists='append')
