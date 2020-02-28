create table rideshare_modeled.Weather as 
select distinct location, time_stamp, temp, clouds, pressure, rain, humidity, wind
from rideshare_staging.Weather
order by location

create table rideshare_modeled.Product_Info as
select distinct cab_type, product_id, name
from rideshare_staging.Lyft_Rides
union all
select distinct cab_type, product_id, name
from rideshare_staging.Uber_Rides

update rideshare_modeled.Product_Info
set id = GENERATE_UUID()
where id is null

create table rideshare_modeled.Ride_Info as
select distinct(source), destination
from rideshare_staging.Lyft_Rides
union all
select distinct(source), destination
from rideshare_staging.Uber_Rides
order by source

-- creates an offense Table -- 
create table rideshare_modeled.offense as 
select distinct OFFENSE_CODE , OFFENSE_CODE_GROUP, INCIDENT_NUMBER,OFFENSE_DESCRIPTION 
from rideshare_staging.Boston_2015_Now
order by OFFENSE_CODE 

-- All the location in one Table -- 
create table rideshare_modeled.location as 
select distinct Long, Lat, Location,STREET,DISTRICT 
from rideshare_staging.Boston_2015_Now
order by Location

-- All the crimes in one Table -- 
create table rideshare_modeled.crime as 
select distinct INCIDENT_NUMBER ,OCCURRED_ON_DATE ,YEAR,MONTH ,DAY_OF_WEEK ,HOUR ,STREET ,SHOOTING 
from rideshare_staging.Boston_2015_Now
order by INCIDENT_NUMBER

-- Tried to cast Shooting at a boolean -- 
SELECT CAST(SHOOTING AS boolean)
FROM rideshare_staging.Boston_2015_Now

