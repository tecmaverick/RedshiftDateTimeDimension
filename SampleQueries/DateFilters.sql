-- ===================================================
-- Get all weekdays
select cal_date from dim_date where is_weekday ='Y' 

-- Get all weekends
select cal_date from dim_date where is_weekday ='N' 

-- Get all Year & month where first day of the month is Monday
select year,month from dim_date where 
day_of_month = 1 and day_of_week = 1 -- 0 (Sunday) to 6 (Saturday)

-- List all leapyears
select distinct year from dim_date where is_leap_year='Y'

