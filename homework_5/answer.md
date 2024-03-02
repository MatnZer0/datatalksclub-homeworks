Question 1:
Install Spark and PySpark

Install Spark
Run PySpark
Create a local spark session
Execute spark.version.
What's the output?

Answer: 3.5.0

Question 2:
FHV October 2019

Read the October 2019 FHV into a Spark Dataframe with a schema as we did in the lessons.

Repartition the Dataframe to 6 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

Answer:

6.MB

Resolution:

!ls -lh fhv/2019/10/

total 27M
-rw-r--r-- 1 jovyan users 3.4M Feb 26 03:31 part-00000-ca39f583-0978-4a66-909c-a1fb20718057-c000.snappy.parquet
-rw-r--r-- 1 jovyan users 3.5M Feb 26 03:31 part-00001-ca39f583-0978-4a66-909c-a1fb20718057-c000.snappy.parquet
-rw-r--r-- 1 jovyan users 3.5M Feb 26 03:31 part-00002-ca39f583-0978-4a66-909c-a1fb20718057-c000.snappy.parquet
-rw-r--r-- 1 jovyan users 3.5M Feb 26 03:31 part-00003-ca39f583-0978-4a66-909c-a1fb20718057-c000.snappy.parquet
-rw-r--r-- 1 jovyan users 3.5M Feb 26 03:31 part-00004-ca39f583-0978-4a66-909c-a1fb20718057-c000.snappy.parquet
-rw-r--r-- 1 jovyan users 3.4M Feb 26 03:31 part-00005-ca39f583-0978-4a66-909c-a1fb20718057-c000.snappy.parquet
-rw-r--r-- 1 jovyan users 3.5M Feb 26 03:31 part-00006-ca39f583-0978-4a66-909c-a1fb20718057-c000.snappy.parquet
-rw-r--r-- 1 jovyan users 2.5M Feb 26 03:31 part-00007-ca39f583-0978-4a66-909c-a1fb20718057-c000.snappy.parquet
-rw-r--r-- 1 jovyan users    0 Feb 26 03:31 _SUCCESS

Question 3:
Count records

How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.

Answer: 62,610

Resolution:

SELECT COUNT(*) FROM fhv_tripdata_2019_10 s WHERE DATE(s.pickup_datetime) = '2019-10-15';

Question 4:
Longest trip for each day

What is the length of the longest trip in the dataset in hours?

Answer: 631152.50

Resolution:

SELECT s.dispatching_base_num, s.pickup_datetime, s."dropOff_datetime", EXTRACT(EPOCH FROM(s."dropOff_datetime" - s.pickup_datetime))/60/60 AS trip_length FROM fhv_tripdata_2019_10 s ORDER BY trip_length DESC LIMIT 10;

 dispatching_base_num |   pickup_datetime   |  dropOff_datetime   |      trip_length      
----------------------+---------------------+---------------------+-----------------------
 B02832               | 2019-10-11 18:00:00 | 2091-10-11 18:30:00 |   631152.500000000000
 B02832               | 2019-10-28 09:00:00 | 2091-10-28 09:30:00 |   631152.500000000000
 B02416               | 2019-10-31 23:46:33 | 2029-11-01 00:13:00 |    87672.440833333333
 B00746               | 2019-10-01 21:43:42 | 2027-10-01 21:45:23 |    70128.028055555556
 B02921               | 2019-10-17 14:00:00 | 2020-10-18 00:00:00 | 8794.0000000000000000
 B03110               | 2019-10-26 21:26:00 | 2020-10-26 21:36:00 | 8784.1666666666666667
 B03080               | 2019-10-30 12:30:04 | 2019-12-30 13:02:08 | 1464.5344444444444500
 B03084               | 2019-10-25 07:04:57 | 2019-12-08 07:54:33 | 1056.8266666666666667
 B03084               | 2019-10-25 07:04:57 | 2019-12-08 07:21:11 | 1056.2705555555555500
 B01452               | 2019-10-01 13:47:17 | 2019-11-03 15:20:28 |  793.5530555555555500

Question 5:
User Interface

Spark’s User Interface which shows the application's dashboard runs on which local port?

Answer: 4040

Question 6:
Least frequent pickup location zone

Load the zone lookup data into a temp view in Spark
Zone Data

Using the zone lookup data and the FHV October 2019 data, what is the name of the LEAST frequent pickup location Zone?

Answer: Jamaica Bay

Resolution:

SELECT z."Zone", COUNT(z."Zone") AS zone_count FROM fhv_tripdata_2019_10 s LEFT JOIN taxi_zone_lookup z ON s."PUlocationID" = z."LocationID" GROUP BY z."Zone" ORDER BY zone_count;


                     Zone                      | zone_count 
-----------------------------------------------+------------
                                               |          0
 Jamaica Bay                                   |          1
 Governor's Island/Ellis Island/Liberty Island |          2
 Green-Wood Cemetery                           |          5
 Broad Channel                                 |          8