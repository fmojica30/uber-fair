-- looks the average price based on the rain amount -- 
create view price_rain as 
select avg(price) as price, w.rain
from `uber-fair.rideshare_modeled.Rider`r ,`uber-fair.weather_modeled.weather` w
group by w.rain

-- compare average price when's raining, uber to lyft
create view lyft_uber_rain as 
select avg(price) as price, avg(rain) as rain, r.cab_type
from `uber-fair.rideshare_modeled.Rider`r ,`uber-fair.weather_modeled.weather` w
where w.rain != 0
group by r.cab_type

-- explore different temp effect on ride price -- 
create view temp_price as 
select t.temp_level , avg(r.price) price
from (select case 
       when w.temp between 0 and 30 then "0-10"
       when w.temp between 31 and 100 then "30-100"
       else "End Range"
  end as temp_level
  from `uber-fair.weather_modeled.weather` w)t, `uber-fair.rideshare_modeled.Rider` r
  group by t.temp_level
  order by t.temp_level




