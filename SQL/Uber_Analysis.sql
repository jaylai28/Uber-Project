-- Find the Total and average fare amount for each Vendor
select "VendorID",
round(sum("fare_amount"),2) as total_fare_amount,
round(avg("fare_amount"),2) as avg_fare_amount 
from uber_data_engineering.staging."fact_table"
group by "VendorID";


-- Find the average tip amount for each payment type
select
b."payment_type_name" as Payment_Type,
round(avg(a."tip_amount"),2) as avg_tip_amount
from uber_data_engineering.staging."fact_table" a
left join UBER_DATA_ENGINEERING b
on a."payment_type_id"=b."payment_type_id"
group by b."payment_type_name";


-- Find the top 10 pickup locations based on the number of trips
select b."pickup_latitude",
b."pickup_longitude",
count(a."trip_id") as number_of_trips
from uber_data_engineering.staging."fact_table" a
join uber_data_engineering.staging."pickup_location_dim" b
on a."pickup_location_id"=b."pickup_location_id"
group by b."pickup_latitude", b."pickup_longitude"
order by number_of_trips DESC
limit 10;


-- Find the number of trips and average tip amount
WITH tip_amount_cte AS (
select
"trip_id",
"tip_amount"

from uber_data_engineering.staging."fact_table"
where "tip_amount"<> 0)

select
count("trip_id") as number_of_trips_with_tip
from tip_amount_cte;

select
round(avg("tip_amount"),2) as avg_tip_amount
from tip_amount_cte;



-- Find the total number of trips per passenger count
select b."passenger_count",
count(a."trip_id") as total_number_of_trips
from uber_data_engineering.staging."fact_table" a
join uber_data_engineering.staging."passenger_count_dim" b
on a."passenger_count_id"=b."passenger_count_id"
group by b."passenger_count"
order by b."passenger_count" asc;


-- Find the average fare amount by hour of the data
select b."pick_hour",
round(avg(a."fare_amount"),2) as avg_fare_amount
from uber_data_engineering.staging."fact_table" a
join uber_data_engineering.staging."datetime_dim" b
on a."datetime_id"=b."datetime_id"
group by b."pick_hour"
order by b."pick_hour";


-- Find the Top 5 busiest hours of the day (i.e. most number of trips in a day)
select b."pick_hour",
count(a."trip_id") as number_of_trips
from uber_data_engineering.staging."fact_table" a
join uber_data_engineering.staging."datetime_dim" b
on a."datetime_id"=b."datetime_id"
group by b."pick_hour"
order by number_of_trips DESC
limit 5;


-- Find the average distance travelled based on the passenger count
select 
c."passenger_count",
-- a."trip_id",
round(avg(b."trip_distance"),2) as avg_trip_distance
from uber_data_engineering.staging."fact_table" a
join uber_data_engineering.staging."trip_distance_dim" b
on a."trip_distance_id"=b."trip_distance_id"
join uber_data_engineering.staging."passenger_count_dim" c
on a."passenger_count_id"=c."passenger_count_id"
group by c."passenger_count"
order by c."passenger_count";


-- Find the total fare amount / trip distance to get the price per distance
with total_price_per_distance as (
select 
(a."total_amount" / b."trip_distance") as price_per_distance
from uber_data_engineering.staging."fact_table" a
join uber_data_engineering.staging."trip_distance_dim" b
on a."trip_distance_id"=b."trip_distance_id"
where b."trip_distance"<>0
)
select
round(
avg(
price_per_distance
),2) as avg_price_per_distance
from total_price_per_distance;