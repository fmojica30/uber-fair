-- find the average hour murder happens-- 
SELECT avg(HOUR)
FROM `rideshare_modeled.crime` c
WHERE c. INCIDENT_NUMBER IN ( SELECT INCIDENT_NUMBER FROM `rideshare_modeled.offense` WHERE OFFENSE_CODE_GROUP = "Homicide")


--Find out what hour the crime is commited based on each crime group--
SELECT avg(q.HOUR)as Hours,q.OFFENSE_CODE_GROUP
FROM
(SELECT c.HOUR,c.INCIDENT_NUMBER,o.OFFENSE_CODE_GROUP
FROM `rideshare_modeled.crime` c
JOIN
(SELECT OFFENSE_CODE_GROUP, INCIDENT_NUMBER
FROM `rideshare_modeled.offense`)o
ON c.INCIDENT_NUMBER = o.INCIDENT_NUMBER)q

group by q.OFFENSE_CODE_GROUP

--Find the average price where the date is not after December 15th --
select avg(price)
from `rideshare_modeled.Rider` r 
where r.time_stamp not in (
  select time_stamp
  from `rideshare_modeled.Weather` w
  where w.time_stamp > 1544917501);
  

--The average price of different times --
select avg(price)
from 
(
  select price, time_stamp
  from rideshare_modeled.Rider
  where price > 3
) r
join 

  select time_stamp
  from rideshare_modeled.Weather
) w
on r.time_stamp = w.time_stamp

group by r.time_stamp
