1. 
```bash
from taxi_pipeline.taxi_data.nyc_taxi_trips
select
	max(trip_pickup_date_time) as end_date,
  min(trip_pickup_date_time) as start_date
limit 100
```

2.
```bash
from taxi_pipeline.taxi_data.nyc_taxi_trips
select
	payment_type,
  count(*) as payment_type_count,
  sum(payment_type_count) over () as total_payment_type_count,
  count(*)/sum(payment_type_count) over () as percentage
group by payment_type
```

3.
```bash
from taxi_pipeline.taxi_data.nyc_taxi_trips
select
	sum(tip_amt)
```