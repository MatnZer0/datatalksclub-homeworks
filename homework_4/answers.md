Question 1:
What happens when we execute dbt build --vars '{'is_test_run':'true'}'
Answer: It's the same as running dbt build

Since the var macro has a default value of true, it will by default execute "limit 100" even if the variable is not declared when running "dbt build".

Question 2:
What is the code that our CI job will run?
Answer: The code that is behind the object on the dbt_cloud_pr_ schema

dbt Cloud builds and tests the models affected by the code change in a temporary schema, unique to the PR. This process ensures that the code builds without error and that it matches the expectations as defined by the project's dbt tests. The unique schema name follows the naming convention dbt_cloud_pr_<job_id>_<pr_id> (for example, dbt_cloud_pr_1862_1704) and can be found in the run details for the given run
source: https://docs.getdbt.com/docs/deploy/continuous-integration

Question 3:
What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)?
Answer: 22998722

CREATE TABLE stg_fhv_tripdata AS
SELECT * FROM fhv_tripdata_2019 WHERE "DOlocationID" IS NOT NULL AND "PUlocationID" IS NOT NULL;

SELECT COUNT(*) 
FROM stg_fhv_tripdata s 
LEFT JOIN taxi_zone_lookup l ON s."PUlocationID" = l."LocationID" 
LEFT JOIN taxi_zone_lookup o ON s."DOlocationID" = o."LocationID"
WHERE l."Borough"<>'Unknown' AND o."Borough"<>'Unknown' AND EXTRACT(year from s."pickup_datetime")='2019';

Question 4:
What is the service that had the most rides during the month of July 2019 month with the biggest amount of rides after building a tile for the fact_fhv_trips table?
Answer: Yellow