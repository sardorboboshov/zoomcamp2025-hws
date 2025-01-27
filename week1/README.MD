1. Params for running ingest_data script
    ```bash
    green_trip_data_url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz" \ 
    zone_lookup_url="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv" \ 
    python ingest_data.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_1=green_tripdata \
    --table_2=zone_lookup \
    --green_trip_data_url="${green_trip_data_url}" \
    --zone_lookup_url="${zone_lookup_url}"
    ```
2. Question 3 query:
    ```sql
    SELECT
        SUM(CASE WHEN trip_distance <= 1 THEN 1 ELSE 0 END) AS up_to_1_mile,
        SUM(CASE WHEN trip_distance > 1 AND trip_distance <= 3 THEN 1 ELSE 0 END) AS between_1_and_3_miles,
        SUM(CASE WHEN trip_distance > 3 AND trip_distance <= 7 THEN 1 ELSE 0 END) AS between_3_and_7_miles,
        SUM(CASE WHEN trip_distance > 7 AND trip_distance <= 10 THEN 1 ELSE 0 END) AS between_7_and_10_miles,
        SUM(CASE WHEN trip_distance > 10 THEN 1 ELSE 0 END) AS over_10_miles
    FROM green_tripdata
    WHERE lpep_pickup_datetime >= '2019-10-01'
    AND lpep_dropoff_datetime < '2019-11-01'
  ```
  the answer is 104,802; 198,924; 109,603; 27,678; 35,189

3. Question 4 query:
    ```sql
    select lpep_pickup_datetime
    from green_tripdata
    where trip_distance = (select max(trip_distance) from green_tripdata)
    ```
    the answer is 2019-10-31

4. Question 5 query:
    ```sql
    SELECT
        zl."Zone", sum(total_amount)
    FROM green_tripdata gt join zone_lookup zl on gt."PULocationID"= zl."LocationID"
    where DATE(lpep_pickup_datetime) = '2019-10-18'
    group by zl."Zone"
    having sum(total_amount) >= 13000 ;
    ```
    East Harlem North, East Harlem South, Morningside Heights

5. Question 6 query:
    ```sql
    with cte as (
        SELECT
        gt."DOLocationID", MAX(tip_amount)
        FROM green_tripdata gt join zone_lookup zl on gt."PULocationID"= zl."LocationID"
        WHERE lpep_pickup_datetime >= '2019-10-01'
            AND lpep_dropoff_datetime < '2019-11-01'
        AND zl."Zone" = 'East Harlem North'
        group by gt."DOLocationID"
        order by MAX(tip_amount) desc
        limit 1
    )
    SELECT "Zone"
    from cte join zone_lookup zl on cte."DOLocationID" = zl."LocationID";
    ```
