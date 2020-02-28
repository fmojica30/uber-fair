-- This query was used to see all the offense with relationship to Firearm Violations
select * from rideshare_staging.Boston_2015_Now where OFFENSE_CODE_GROUP = "Firearm Violations" order by YEAR;
-- This query was used to see all the crimes that happened on Bennington St
select * from rideshare_staging.Boston_2015_Now where street = "BENNINGTON ST" order by YEAR;
