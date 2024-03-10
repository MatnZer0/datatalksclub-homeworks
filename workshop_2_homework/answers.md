Question 1
Create a materialized view to compute the average, min and max trip time between each taxi zone.

Note that we consider the do not consider a->b and b->a as the same trip pair. So as an example, you would consider the following trip pairs as different pairs:

Yorkville East -> Steinway
Steinway -> Yorkville East
From this MV, find the pair of taxi zones with the highest average trip time. You may need to use the dynamic filter pattern for this.

Answer:

Yorkville East, Steinway

Resolution:

CREATE MATERIALIZED VIEW stats_pair_zone_trip AS
    WITH t AS (
        SELECT 
            l.zone as pickup_zone,
            o.zone as dropoff_zone,
            EXTRACT(EPOCH FROM(s.tpep_dropoff_datetime - s.tpep_pickup_datetime)) AS trip_time
        FROM 
            trip_data s 
            LEFT JOIN taxi_zone l ON s.pulocationid = l.location_id 
            LEFT JOIN taxi_zone o ON s.dolocationid = o.location_id
    )
    SELECT 
        pickup_zone, dropoff_zone,
        AVG(t.trip_time) AS avg_trip_time, 
        MIN(trip_time) as min_trip_time, 
        MAX(trip_time) as max_trip_time,
        COUNT(*) AS record_count
    FROM t
    GROUP BY pickup_zone, dropoff_zone
    ORDER BY avg_trip_time DESC;

SELECT * FROM stats_pair_zone_trip
ORDER BY avg_trip_time DESC;

             pickup_zone             |            dropoff_zone             |         avg_trip_time          | min_trip_time | max_trip_time | record_count 
-------------------------------------+-------------------------------------+--------------------------------+---------------+---------------+--------------
 Yorkville East                      | Steinway                            |                   86373.000000 |  86373.000000 |  86373.000000 |            1
 Stuy Town/Peter Cooper Village      | Murray Hill-Queens                  |                   86324.000000 |  86324.000000 |  86324.000000 |            1
 Washington Heights North            | Highbridge Park                     |                   86320.000000 |  86320.000000 |  86320.000000 |            1
 Two Bridges/Seward Park             | Bushwick South                      |                   86294.000000 |  86294.000000 |  86294.000000 |            1
 Clinton East                        | Prospect-Lefferts Gardens           |                   86036.000000 |  86036.000000 |  86036.000000 |            1
 SoHo                                | South Williamsburg                  |                   85798.000000 |  85798.000000 |  85798.000000 |            1
 Downtown Brooklyn/MetroTech         | Garment District                    |                   85303.000000 |  85303.000000 |  85303.000000 |            1
 Lower East Side                     | Sunset Park West                    |                   75034.000000 |  75034.000000 |  75034.000000 |            1
 West Village                        | Flatbush/Ditmas Park                |                   43398.000000 |   1212.000000 |  85584.000000 |            2


Question 2
Recreate the MV(s) in question 1, to also find the number of trips for the pair of taxi zones with the highest average trip time.

Answer: 1
Resolution: same resolution to question 1

Question 3
From the latest pickup time to 17 hours before, what are the top 3 busiest zones in terms of number of pickups? For example if the latest pickup time is 2020-01-01 17:00:00, then the query should return the top 3 busiest zones from 2020-01-01 00:00:00 to 2020-01-01 17:00:00.

Answer: LaGuardia Airport, Lincoln Square East, JFK Airport

Resolution:

CREATE MATERIALIZED VIEW latest_pickup AS
    SELECT
        max(tpep_pickup_datetime) AS latest_pickup_time
    FROM
        trip_data;

CREATE MATERIALIZED VIEW busiest_zones AS
    SELECT
        taxi_zone.Zone,
        count(*) AS last_17_hours_dropoff_cnt
    FROM
        trip_data
            JOIN taxi_zone
                ON trip_data.puLocationID = taxi_zone.location_id
            JOIN latest_pickup
                ON trip_data.tpep_pickup_datetime > latest_pickup.latest_pickup_time - interval '17 hour'
    GROUP BY
        taxi_zone.Zone
    ORDER BY last_17_hours_dropoff_cnt DESC
        LIMIT 3;

SELECT * FROM busiest_zones;

        zone         | last_17_hours_dropoff_cnt 
---------------------+---------------------------
 LaGuardia Airport   |                        19
 JFK Airport         |                        17
 Lincoln Square East |                        17