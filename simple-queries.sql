-- This query was used to see what date and time there was rain present in the area along with the amount of humidity in ascending order
select * from rideshare_staging.Weather where rain > 0 order by humidity;

-- This query was used in order to see the amount of Uber Black rides ordered within the data and sorted by price in descending order
select * from rideshare_staging.Uber_Rides where name = "Black" order by price desc;

--This query was run  to see the price Lyft rides that had a destination of Black Bay being orderd by their time in ascending order
select price, time_stamp from rideshare_staging.Lyft_Rides where destination = "Black Bay" order by time_stamp;

-- This query was used to see all the offense with relationship to Firearm Violations
select * from rideshare_staging.Boston_2015_Now where OFFENSE_CODE_GROUP = "Firearm Violations" order by YEAR;
-- This query was used to see all the crimes that happened on Bennington St
select * from rideshare_staging.Boston_2015_Now where street = "BENNINGTON ST" order by YEAR;
