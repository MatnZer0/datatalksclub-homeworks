Question 1: Redpanda version
What's the version, based on the output of the command you executed? (copy the entire version)

Answer: 
v22.3.5 (rev 28b2443)

Resolution:
docker exec -it redpanda-1 /bin/bash
rpk version

Question 2. Creating a topic
What's the output of the command for creating a topic? Include the entire output in your answer.

Answer:
TOPIC       STATUS
test-topic  OK

Resolution:
rpk topic create test-topic

Question 3. Connecting to the Kafka server
Provided that you can connect to the server, what's the output of the last command?

Answer:
True

Resolution:
question3.py output

Question 4. Sending data to the stream
How much time did it take? Where did it spend most of the time?

Answer:
Sending the messages

Resolution:
question4.py output

Question 5: Sending the Trip Data
How much time in seconds did it take? (You can round it to a whole number)

Answer:
took 241.53 seconds sending the messages

Resolution:
question5.py output

Question 6. Parsing the data
How does the record look after parsing? Copy the output.

Answer:
Row(VendorID=None, lpep_pickup_datetime='2019-10-01 00:26:02', lpep_dropoff_datetime='2019-10-01 00:39:58', PULocationID=112, DOLocationID=196, passenger_count=1.0, trip_distance=5.88, tip_amount=0.0)

Resolution:
question6.ipynb output

Question 7: Most popular destination
Write the most popular destination, your answer should be either the zone ID or the zone name of this destination. (You will need to re-send the data for this to work)

Answer:
74

Resolution:
question7.py output:
+------------------------------------------+------------+-----+
|window                                    |DOLocationID|count|
+------------------------------------------+------------+-----+
|{2024-04-07 19:55:00, 2024-04-07 20:00:00}|74          |34156|
|{2024-04-07 19:55:00, 2024-04-07 20:00:00}|42          |30645|
|{2024-04-07 19:55:00, 2024-04-07 20:00:00}|41          |27044|
|{2024-04-07 19:55:00, 2024-04-07 20:00:00}|75          |24843|
|{2024-04-07 19:55:00, 2024-04-07 20:00:00}|129         |22945|
|{2024-04-07 19:55:00, 2024-04-07 20:00:00}|7           |22132|
|{2024-04-07 19:55:00, 2024-04-07 20:00:00}|166         |21012|
|{2024-04-07 19:55:00, 2024-04-07 20:00:00}|236         |15294|
|{2024-04-07 19:55:00, 2024-04-07 20:00:00}|223         |14463|
|{2024-04-07 19:55:00, 2024-04-07 20:00:00}|238         |14117|
|{2024-04-07 19:55:00, 2024-04-07 20:00:00}|82          |13997|
|{2024-04-07 19:55:00, 2024-04-07 20:00:00}|181         |13941|
|{2024-04-07 19:55:00, 2024-04-07 20:00:00}|95          |13874|
|{2024-04-07 19:55:00, 2024-04-07 20:00:00}|244         |12987|
|{2024-04-07 19:55:00, 2024-04-07 20:00:00}|61          |12647|
|{2024-04-07 19:55:00, 2024-04-07 20:00:00}|116         |12183|
|{2024-04-07 19:55:00, 2024-04-07 20:00:00}|138         |11896|
|{2024-04-07 19:55:00, 2024-04-07 20:00:00}|97          |11618|
|{2024-04-07 19:55:00, 2024-04-07 20:00:00}|49          |9977 |
|{2024-04-07 19:55:00, 2024-04-07 20:00:00}|151         |9878 |
+------------------------------------------+------------+-----+
