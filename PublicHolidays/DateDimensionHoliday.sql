-- ************************************************************************************************************
-- SQL Script to populate Date Dimension table with federal holidays from 1970-01-01 till 2069-12-31 (100 Years)
-- Author: Abraham
-- Created-on: 21th March 2022

-- Change log:
-- Modified-on: 
-- Changes:
-- ************************************************************************************************************


--*********************************************************************************************************
-- Instructions before script exeuction
-- Update S3 bucket name place holder "replace_with_s3_bucket_name" before executing the script
--*********************************************************************************************************

BEGIN TRANSACTION;

-- ===================================================
-- Create table to hold the holiday list from CSV
drop table if exists public.holidays;

create table public.holidays 
(
    holidaydate date, 
    US_holidays varchar(50),
    Australia_holidays varchar(50)
);

-- ===================================================
-- Load data from CSV to holiday table
-- Make sure the S3 bucket is in the same region as Redshift Cluster

COPY public.holidays from 's3://replace_with_s3_bucket_name/Holidays.csv'
iam_role default
FORMAT CSV
DATEFORMAT AS 'YYYY-MM-DD'
BLANKSASNULL
TRIMBLANKS
TRUNCATECOLUMNS
IGNOREHEADER 1;

-- ===================================================
-- Update Date dimension table with data from holiday list

update dim_date dd 
set 
   	au_holiday_name = h.australia_holidays,
    us_holiday_name = h.us_holidays
from public.holidays h where 
dd.cal_date = h.holidaydate;

-- Update US holiday 'Martin Luther King Jr. Day' which falls on Monday third week of every Jan from 1986 onwards.
update dim_date dd 
set 
    us_holiday_name = 'Martin Luther King Jr. Day'
where 
    dd.week_of_month = 3 and 
    dd.month = 1 and
    dd.day_of_week = 1 and
	year>=1986;

-- ------------------------------------------------------------------------------------------------------------
-- Update US holiday 'Juneteenth National Independence Day' falls 19th Jun every year.
-- If it falls on a Saturday, the preceding Friday will be treated\observed as a holiday. 
-- If it holiday falls on a Sunday,  the following Monday will be treated\observed as a holiday.
-- https://www.opm.gov/policy-data-oversight/pay-leave/federal-holidays

-- Set 19th Jun every year as 'Juneteenth National Independence Day'
update dim_date dd 
set 
    us_holiday_name = 'Juneteenth National Independence Day'
where 
    dd.month = 6 and 
    dd.day_of_month = 19;

    
-- Get all 'Juneteenth National Independence Day' on Saturday, and create a observance on Friday
with us_juneteenth_on_saturday as (
    select id_seq - 1 as newid 
  		from dim_date 
    where 
  		us_holiday_name = 'Juneteenth National Independence Day' and 
    	month = 6 and day_of_month = 19 and day_of_week = 6
)
update dim_date 
	set us_holiday_name = 'Juneteenth National Independence Day (Observance)'
from 
	us_juneteenth_on_saturday j
where 
	j.newid = dim_date.id_seq;

-- Get all 'Juneteenth National Independence Day' on Sunday, and create a observance on Monday
with us_juneteenth_on_sunday as (
    select id_seq + 1 as newid 
  		from dim_date 
    where 
  		us_holiday_name = 'Juneteenth National Independence Day' and 
    	month = 6 and day_of_month = 19 and day_of_week = 0
)
update dim_date 
	set us_holiday_name = 'Juneteenth National Independence Day (Observance)'
from 
	us_juneteenth_on_sunday j
where 
	j.newid = dim_date.id_seq;
-- ------------------------------------------------------------------------------------------------------------

-- Update field 'au_is_holiday'
update dim_date set au_is_holiday='Y' where au_holiday_name is not null;

-- Update field 'us_is_holiday'
update dim_date set us_is_holiday='Y' where us_holiday_name is not null;

-- Delete holiday table after date dimension table update
drop table public.holidays;

COMMIT TRANSACTION ;




