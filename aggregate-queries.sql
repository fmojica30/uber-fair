--This query counts gives us the amount of times that a rider took a ride of a certain distance
select distance, count(price)
from rideshare_modeled.Rider
group by distance
having distance < 5.0;

--This query shows us the average distance for each price of ride in our data
select price, avg(distance)
from rideshare_modeled.Rider r
group by price
having price > 0
order by price;

--This query shows us the average temperature in each of the different locations that we are tracking 
select source, avg(temp)
from rideshare_modeled.Ride_Info r
join rideshare_modeled.Weather w
on r.source = w.location
group by source

-- This check average time when crime happends on different scrent 
SELECT AVG(HOUR),street
FROM `uber-fair.rideshare_modeled.crime`
WHERE YEAR = 2019
GROUP BY STREET

-- Find out how many shooting happened on each street -- 
SELECT count(year) Shooting,year
FROM `uber-fair.rideshare_modeled.crime`
WHERE Shooting IS NOT null and STREET IS NOT null
GROUP BY year

-- Find street that have more 
SELECT count(street),STREET 
FROM `uber-fair.rideshare_modeled.crime`
WHERE Shooting IS NOT null and STREET IS NOT null
GROUP BY STREET
Having count(shooting)>5

