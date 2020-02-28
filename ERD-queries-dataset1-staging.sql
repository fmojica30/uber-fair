-- Find Incident Number match --
SELECT COUNT (DISTINCT INCIDENT_NUMBER)
FROM rideshare_staging.Boston_2015_Now

-- Find How much incident number there are -- 
SELECT COUNT ( INCIDENT_NUMBER)
FROM rideshare_staging.Boston_2015_Now

select count(*) from rideshare_staging.Lyft_Rides 

select count(*) from rideshare_staging.Weather

select count(distinct id) from rideshare_staging.Lyft_Rides 

select count(distinct time_stamp) from rideshare_staging.Weather