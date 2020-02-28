-- Gets where the crime and lyft rides overlapes
SELECT OFFENSE_DESCRIPTION, OCCURRED_ON_DATE, DISTRICT, INCIDENT_NUMBER
FROM rideshare_staging.Boston_2015_Now B
LEFT JOIN rideshare_staging.Lyft_Rides L
ON B.STREET = L.destination

-- Gets where the crime and Uber rides overlapes
SELECT OFFENSE_DESCRIPTION, OCCURRED_ON_DATE, DISTRICT, INCIDENT_NUMBER
FROM rideshare_staging.Boston_2015_Now B
LEFT JOIN rideshare_staging.Uber_Rides U
ON B.STREET = U.destination

-- Joins where the crime happened and weather for location
SELECT OFFENSE_DESCRIPTION, OCCURRED_ON_DATE, DISTRICT, INCIDENT_NUMBER
FROM rideshare_staging.Boston_2015_Now B
LEFT JOIN rideshare_staging.Weather W
ON B.STREET = W.location

-- join time stamp with weather to get all the times the price is over 30 -- 
select id, l.price, l.time_stamp 
from rideshare_staging.Lyft_Rides l
left join rideshare_staging.Weather w on l.time_stamp = w.time_stamp
where l.price > 30;

-- Compare the price of uber and lyft rides based on distance -- 
select l.price, l.distance, u.price as uber_price, u.distance as uber_distance
from rideshare_staging.Lyft_Rides l
left join rideshare_staging.Uber_Rides u on l.distance = u.distance

-- See the relationship between weather and lyft price when price is greater than 10 --
select id, l.price, l.time_stamp 
from rideshare_staging.Lyft_Rides l
left join rideshare_staging.Weather w on l.time_stamp = w.time_stamp
where l.price > 10




