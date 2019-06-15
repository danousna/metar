/*
  Moyenne de feel par jour pour une position et une année donnée
 */

select year, month, day, avg(feel) as feel
from date_by_location
where lat = 52.5644 AND lon = 13.3088 AND year = 2011
group by year, month, day;


select month, day, avg(feel) as feel
from date_by_location
where lat = 52.5644 AND lon = 13.3088
group by month, day;