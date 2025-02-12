-- 
CREATE OR REPLACE EXTERNAL TABLE `external_table` (
        VendorID INTEGER OPTIONS (
            description = 'A code indicating the LPEP provider that provided the record. 1= Creative Mobile Technologies, LLC; 2= VeriFone Inc.'
        ),
        tpep_pickup_datetime TIMESTAMP OPTIONS (
            description = 'The date and time when the meter was engaged'
        ),
        tpep_dropoff_datetime TIMESTAMP OPTIONS (
            description = 'The date and time when the meter was disengaged'
        ),
        passenger_count INTEGER OPTIONS (
            description = 'The number of passengers in the vehicle. This is a driver-entered value.'
        ),
        trip_distance FLOAT64 OPTIONS (
            description = 'The elapsed trip distance in miles reported by the taximeter.'
        ),
        RatecodeID INTEGER OPTIONS (
            description = 'The final rate code in effect at the end of the trip. 1= Standard rate 2=JFK 3=Newark 4=Nassau or Westchester 5=Negotiated fare 6=Group ride'
        ),
        store_and_fwd_flag STRING OPTIONS (
            description = 'This flag indicates whether the trip record was held in vehicle memory before sending to the vendor, aka "store and forward," because the vehicle did not have a connection to the server. TRUE = store and forward trip, FALSE = not a store and forward trip'
        ),
        PULocationID INTEGER OPTIONS (
            description = 'TLC Taxi Zone in which the taximeter was engaged'
        ),
        DOLocationID INTEGER OPTIONS (
            description = 'TLC Taxi Zone in which the taximeter was disengaged'
        ),
        payment_type INTEGER OPTIONS (
            description = 'A numeric code signifying how the passenger paid for the trip. 1= Credit card 2= Cash 3= No charge 4= Dispute 5= Unknown 6= Voided trip'
        ),
        fare_amount FLOAT64 OPTIONS (
            description = 'The time-and-distance fare calculated by the meter'
        ),
        extra FLOAT64 OPTIONS (
            description = 'Miscellaneous extras and surcharges. Currently, this only includes the $0.50 and $1 rush hour and overnight charges'
        ),
        mta_tax FLOAT64 OPTIONS (
            description = '$0.50 MTA tax that is automatically triggered based on the metered rate in use'
        ),
        tip_amount FLOAT64 OPTIONS (
            description = 'Tip amount. This field is automatically populated for credit card tips. Cash tips are not included.'
        ),
        tolls_amount FLOAT64 OPTIONS (
            description = 'Total amount of all tolls paid in trip.'
        ),
        improvement_surcharge FLOAT64 OPTIONS (
            description = '$0.30 improvement surcharge assessed on hailed trips at the flag drop. The improvement surcharge began being levied in 2015.'
        ),
        total_amount FLOAT64 OPTIONS (
            description = 'The total amount charged to passengers. Does not include cash tips.'
        ),
        congestion_surcharge FLOAT64 OPTIONS (
            description = 'Congestion surcharge applied to trips in congested zones'
        )
    ) OPTIONS (
        format = 'PARQUET',
        uris = [
                'file_1',
                'file_2',
                'file_3',
                'file_4',
                'file_5',
                'file_6'
              ]
    );
-- 
-- 
CREATE OR REPLACE TABLE `table` AS
SELECT *
FROM `external_table`;
-- 
CREATE OR REPLACE MATERIALIZED VIEW `materialized_table` AS
SELECT *
FROM `table`;
-- 
SELECT COUNT(DISTINCT PULocationID)
FROM `external_table`;
SELECT COUNT(DISTINCT PULocationID)
FROM `table`;
-- 
SELECT COUNT(*)
FROM `table`
where fare_amount = 0;
-- 
CREATE OR REPLACE TABLE `optimized_table` PARTITION BY DATE(tpep_dropoff_datetime) CLUSTER BY VendorId AS
SELECT *
FROM `table`;
--
SELECT COUNT(DISTINCT VendorId)
FROM `materialized_table`
WHERE tpep_dropoff_datetime >= '2024-03-01'
    and tpep_dropoff_datetime <= '2024-03-15';
--
SELECT COUNT(DISTINCT VendorId)
FROM `optimized_table`
WHERE tpep_dropoff_datetime >= '2024-03-01'
    and tpep_dropoff_datetime <= '2024-03-15';