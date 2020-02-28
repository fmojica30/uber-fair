--based on the time, look at the price and temp -- 
select temp,price 
from `weather_modeled.weather` w
full join `rideshare_modeled.Rider`r on w.time_stamp =r.time_stamp

-- look at the average price when it's not raining -- 
select avg(price)
FROM `rideshare_modeled.Rider` r
FULL JOIN `weather_modeled.weather` w on  w.time_stamp =r.time_stamp
WHERE w.rain is null

-- look at the average price when it's raining -- 
select avg(price)
FROM `rideshare_modeled.Rider` r
FULL JOIN `weather_modeled.weather` w on  w.time_stamp =r.time_stamp
WHERE w.rain is not null


