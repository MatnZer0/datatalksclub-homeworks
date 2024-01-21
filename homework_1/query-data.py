import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://root:root@localhost:5432/ny_taxi')

engine.connect()

query = """
SELECT green_tripdata_trip.tip_amount, tz1."Zone", tz2."Zone"
FROM green_tripdata_trip
LEFT JOIN taxi_zone_lookup AS tz1
ON "PULocationID" = tz1."LocationID"
LEFT JOIN taxi_zone_lookup AS tz2
ON "DOLocationID" = tz2."LocationID"
WHERE tz1."Zone" = 'Astoria'
ORDER BY tip_amount DESC
"""

query_result = pd.read_sql(query, con=engine)
print(query_result)