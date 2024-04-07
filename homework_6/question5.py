import json
import time
from kafka import KafkaProducer
import pandas as pd
import pyspark
from pyspark.sql import types

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

df_green = pd.read_csv('green_tripdata_2019-10.csv')

topic_name = 'green-trips'

t0 = time.time()

for row in df_green.itertuples(index=False):
    row_dict = {col: getattr(row, col) for col in row._fields}
    message = row_dict
    producer.send(topic_name, value=message)
    print(f"Sent: {message}")

producer.flush()

t1 = time.time()

print(f'took {(t1 - t0):.2f} seconds sending the messages')
