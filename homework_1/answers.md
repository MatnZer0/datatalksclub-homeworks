Question 1. Docker tags
A: --rm
Resolution:

docker run --help

Usage:  docker run [OPTIONS] IMAGE [COMMAND] [ARG...]
Create and run a new container from an image
Aliases:
  docker container run, docker run
Options:

--rm Automatically remove the container when it exits

Question 2. Docker run: version of wheel
A: 0.42.0
Resolution:

docker run -it --rm --entrypoint /bin/bash python:3.9
pip list

Question 3. Count records
A: 15612
Resolution:

query:
"
SELECT count(1) FROM green_tripdata_trip
WHERE TO_CHAR("lpep_pickup_datetime", 'YYYY-MM-DD') = '2019-09-18'
AND TO_CHAR("lpep_dropoff_datetime", 'YYYY-MM-DD') = '2019-09-18';
"

Question 4. Largest trip for each day
A: 2019-09-26
Resolution:

query:
"
SELECT MAX(trip_distance) FROM green_tripdata_trip
WHERE TO_CHAR("lpep_pickup_datetime", 'YYYY-MM-DD') = '2019-09-26'
"

2019-09-18: 70.28
2019-09-16: 114.3
2019-09-26: 341.64
2019-09-21: 135.53

Question 5. Three biggest pick up Boroughs
A: "Brooklyn" "Manhattan" "Queens"
Resolution:

query = """
SELECT SUM(green_tripdata_trip.total_amount), "Borough", COUNT(1)
FROM green_tripdata_trip
LEFT JOIN taxi_zone_lookup
ON "PULocationID" = "LocationID"
WHERE TO_CHAR("lpep_pickup_datetime", 'YYYY-MM-DD') = '2019-09-18'
GROUP BY "Borough"
ORDER BY sum DESC
"""

        sum        Borough  count
0  96333.24       Brooklyn   4458
1  92271.30      Manhattan   5575
2  78671.71         Queens   4393
3  32830.09          Bronx   1308
4    728.75        Unknown     24
5    342.59  Staten Island      9

Question 6. Largest tip
A: JFK Airport
Resolution:

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

       tip_amount     Zone                   Zone
0           62.31  Astoria            JFK Airport
1           30.00  Astoria               Woodside
2           28.00  Astoria               Kips Bay
3           25.00  Astoria                     NV
4           20.00  Astoria  Upper West Side South
...           ...      ...                    ...
18274        0.00  Astoria               Woodside
18275        0.00  Astoria                 Corona
18276        0.00  Astoria                Astoria
18277        0.00  Astoria        Oakland Gardens
18278        0.00  Astoria               Elmhurst

Question 7. Creating Resources

(env) [matn0@matn0manj terraform]$ terraform apply
google_storage_bucket.demo-bucket: Refreshing state... [id=terraform-datatalks-terra-bucket]
google_bigquery_dataset.demo_dataset: Refreshing state... [id=projects/terraform-datatalks/datasets/demo_dataset]

Terraform used the selected providers to generate the following execution plan. Resource actions are indicated with the following symbols:
  ~ update in-place

Terraform will perform the following actions:

  # google_bigquery_dataset.demo_dataset will be updated in-place
  ~ resource "google_bigquery_dataset" "demo_dataset" {
      - default_partition_expiration_ms = 5184000000 -> null
      - default_table_expiration_ms     = 5184000000 -> null
        id                              = "projects/terraform-datatalks/datasets/demo_dataset"
        # (12 unchanged attributes hidden)

        # (4 unchanged blocks hidden)
    }

Plan: 0 to add, 1 to change, 0 to destroy.

Do you want to perform these actions?
  Terraform will perform the actions described above.
  Only 'yes' will be accepted to approve.

  Enter a value: yes

google_bigquery_dataset.demo_dataset: Modifying... [id=projects/terraform-datatalks/datasets/demo_dataset]
google_bigquery_dataset.demo_dataset: Modifications complete after 1s [id=projects/terraform-datatalks/datasets/demo_dataset]

Apply complete! Resources: 0 added, 1 changed, 0 destroyed.