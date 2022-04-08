-- ************************************************************************************************************
-- SQL Script to create AWS Redshift Date Dimension table. Populates date from 1970-01-01 till 2069-12-31 (100 Years)
-- Author: Abraham
-- Created-on: 21th March 2022

-- Change log:
-- Modified-on: 
-- Changes:
-- ************************************************************************************************************

--*********************************************************************************************************
-- Instructions before script exeuction
-- 
--*********************************************************************************************************


-- ************************************************************************************************************
-- seq (sequence) fields with seq are numbers 1..n from the start year 1970 till end year 2069. 
--                Grouped by respective attributes like year, month, week, day etc
-- 'key' prefixedfields are text fields concatenating values from multiple fields
-- dod - Day Over Day
-- wow - Week Over Week
-- mom - Month Over Month
-- qoq - Quarter Over Quarter
-- sos - Semester Over Semester
-- yoy - Year Over Year
-- ly  -  Last Year

BEGIN TRANSACTION;

--  Setting timezone to UTC for the session
SET timezone to default;

DROP TABLE IF EXISTS public.dim_date;

-- Create Date dimension table.
-- SORT Key - cal_date 
-- DISTSYLE ALL (Deciding factors - Small dimension table. No changes. Reduces inter-node broadcast)
-- ENCODING - Set to RAW for sort keys and for remaining fields, left to Redshift to decide
CREATE TABLE public.dim_date(
  id_seq integer NOT NULL,  -- Number sequence from 1 to total number of rows in the table
  
  cal_date              date sortkey NOT NULL,  -- Calendar Date datatype
  date_epoch            bigint NOT NULL,        -- Values in seconds for given date. Unix epoch the number of seconds that have elapsed since January 1, 1970 (midnight UTC/GMT)

  day_of_week           smallint NOT NULL,      -- 0 (Sunday) to 6 (Saturday)
  day_name_of_week      varchar(10) NOT NULL,   -- Monday, Tuesday ....
  day_of_month          smallint NOT NULL,      -- 1,2,3 ... 31 till month end
  day_of_quarter        smallint NOT NULL,      -- 1,2,3 ... 90 till quarter end
  day_of_year           smallint NOT NULL,      -- 1,2,3... 366
  is_weekday            char(1) NOT NULL,       -- Day is weekday (Y,N)
  day_of_month_suffix   char(2) NOT NULL,       -- st,nd,rd,th based on 1st day of the month
  dod                   date NOT NULL,          -- Day Over Day (Prior Day)
  business_day_seq      smallint NOT NULL,      -- Number from 1 to total number of records in the date dimension table.
                                                -- The number is only applicable for ONLY weekdays, doesn't account for public holidays
                                                -- For public holidays use country specific fields 'au_business_day_seq' and 'us_business_day_seq'

  week_of_year          smallint NOT NULL,      -- Week of the Year. 1 to 52,53; the first week starts on the first day of the year
  week_of_year_iso      smallint NOT NULL, 	    -- ISO week number of year (the first Thursday of the new year is in week 1.)
  week_of_month         smallint NOT NULL,      -- Week of the month starting at 1st of each month. Every seven day is counted as week and goes from 1 to 5
  week_start_date       date NOT NULL,          -- First date of the week, starting with Monday (Week To Date - WTD)
  week_end_date         date NOT NULL,          -- Last date of the week, ending with Sunday
  wow_start_date        date NOT NULL,          -- Week Over Week calendar start date (Prior week start)
  wow_end_date          date NOT NULL,          -- Week Over Week calendar start date (Prior week end)
  week_of_month_suffix  char(2) NOT NULL,       -- Values: st,nd,rd,th based on week of the month
  week_of_year_suffix   char(2) NOT NULL,       -- Values: st,nd,rd,th based on week of the year
  week_of_year_iso_suffix char(2) NOT NULL,     -- Values: st,nd,rd,th based on week of the year ISO
  week_seq              smallint NOT NULL,      -- Sequence of numbers by week, begining from calendar start date till end.  Eg 1970-01-01 till 2050-01-01
  week_iso_seq          smallint NOT NULL,      -- Sequence of numbers by ISO week, begining from calendar start date till end.  Eg 1970-01-01 till 2050-01-01

  month                 smallint NOT NULL,      -- Day of the month from 1 .. 29,30,31
  month_suffix          char(2) NOT NULL,       -- st,nd,rd,th. based on year, like 1st Month, 2nd Month
  month_name            varchar(9) NOT NULL,    -- January, February.....
  month_name_abbrv      varchar(3) NOT NULL,    -- Jan, Feb, Mar
  month_start_date      date NOT NULL,          -- First day of the month. Example 2022-03-01  (Month To Date - MTD)
  month_end_date        date NOT NULL,          -- Last day of the month. Example 2022-03-31
  mom_start_date        date NOT NULL,          -- Month Over Month start date (Prior Month)
  mom_end_date          date NOT NULL,          -- Month Over Month end date (Prior Month)
  month_seq             smallint NOT NULL,      -- Sequence of numbers by month, begining from calendar start date till end.  Eg 1970-01-01 till 2050-01-01
  month_of_quarter      smallint NOT NULL,      -- Month from 1 to 3 for each quarter.


  quarter               smallint NOT NULL,      -- Calendar quarter number 1,2,3,4
  quarter_start_date    date NOT NULL,          -- Calenday quarter start date
  quarter_end_date      date NOT NULL,          -- Calenday quarter end date
  quarter_abbrv         char(2) NOT NULL,       -- Quarter abbreviation Q1, Q2, Q3, Q4
  qoq_start_date        date NOT NULL,          -- Quarter Over Quarter start date (Prior Quarter)
  qoq_end_date          date NOT NULL,          -- Quarter Over Quarter end date (Prior Quarter)
  quarter_seq           smallint NOT NULL,      -- Sequence of numbers by quarter, begining from calendar start date till end.  Eg 1970-01-01 till 2050-01-01

  semester_id           smallint NOT NULL,      -- Semster of the year 1,2
  semester_start_date   date NOT NULL,          -- Semster start date for the year Ex. 2020-01-01 (Semster To Date)
  semester_end_date     date NOT NULL,          -- Semster start date for the year Ex. 2020-06-30
  sos_start_year        date NOT NULL,          -- Semester over Semester (Prior sem start date)
  sos_end_year          date NOT NULL,          -- Semester over Semester (Prior sem end date)
  day_of_semester       smallint NOT NULL,      -- Day of semster 1...180
  semester_abbrv        char(2) NOT NULL,       -- S1,S2
  semester_seq          smallint NOT NULL,      -- Sequence of numbers by semester, begining from calendar start date till end.  Eg 1970-01-01 till 2050-01-01

  year                  smallint NOT NULL,      -- Current year from date
  year_short            smallint NOT NULL,      -- Short current year from date. Eg 22 (instead of 2022)
  year_iso              smallint NOT NULL,      -- ISO Year from date
  year_first_day        date NOT NULL,          -- First date of the year Example 2021-01-01 (Year To Date - YTD)
  year_last_day         date NOT NULL,          -- Last date of the year  Example 2021-12-31
  is_leap_year          char(1) NOT NULL,       -- Values 'Y', 'N' Logic: (Year mod 4 = 0) AND ((Year mod 100 != 0) or (Year mod 400 = 0)))
  year_seq              smallint NOT NULL,      -- Sequence of numbers by year, begining from calendar start date till end.  Eg 1970-01-01 till 2050-01-01

  key_date              char(8) NOT NULL,       -- 20210231 (YYYYMMDD)
  key_short_date        char(6) NOT NULL,       -- 210231 (YYMMDD)
  key_month             char(6) NOT NULL,       -- 202101 (YYYYMM)
  key_short_month       char(4) NOT NULL,       -- 2101 (YYMM)
  key_qaurter           char(6) NOT NULL,       -- 2021Q2
  key_short_qaurter     char(4) NOT NULL,       -- 21Q2
  key_semester          char(6) NOT NULL,       -- 2021S1
  key_short_semester    char(4) NOT NULL,       -- 21S1
  key_week_year         char(6) NOT NULL,       -- 202153 (YYYYWW)
  key_short_week_year   char(6) NOT NULL,       -- 2153 (YYWW)
  key_week_iso_year     char(6) NOT NULL,       -- 202152 (YYYYIW)
  key_short_week_iso_year char(4) NOT NULL,     -- 2152 (YYIW)

  prior_360d_from_date date NOT NULL,     -- Current date - 360 days
  prior_180d_from_date date NOT NULL,     -- Current date - 180 days
  prior_120d_from_date date NOT NULL,     -- Current date - 120 days
  prior_90d_from_date  date NOT NULL,     -- Current date - 90 days
  prior_60d_from_date  date NOT NULL,     -- Current date - 60 days
  prior_30d_from_date  date NOT NULL,     -- Current date - 30 days
  prior_14d_from_date  date NOT NULL,     -- Current date - 14 days
  prior_7d_from_date   date NOT NULL,     -- Current date - 7 days

  ly_ytd_start_date   date NOT NULL,    -- LY (Last Year) YTD start date. If current date is 2020-03-01 then, ly_ytd_start_date will be 2019-01-01
  ly_ytd_end_date     date NOT NULL,    -- LY YTD end date (same day as current date). If current date is 2020-03-01 then, ly_ytd_end_date will be 2019-03-01
  ly_std_start_date   date NOT NULL,    -- LY STD (Semester To Date) start date
  ly_std_end_date     date NOT NULL,    -- LY STD end date
  ly_qtd_start_date   date NOT NULL,    -- LY Qtr start date
  ly_qtd_end_date     date NOT NULL,    -- LY Qtr end date
  ly_mtd_start_date   date NOT NULL,    -- LY MTD start date      
  ly_mtd_end_date     date NOT NULL,    -- LY MTD end date      
  ly_mtd_end_date_actual date NOT NULL, -- LY MTD end date balanced for leap year additional day  
  ly_wtd_start_date   date NOT NULL,    -- LY WTD start date      
  ly_wtd_end_date     date NOT NULL,    -- LY WTD end date      

  ly_yoy_start_date   date NOT NULL,    -- LY (Last Year) YoY (Year Over Year) start date
  ly_yoy_end_date     date NOT NULL,    -- LY YoY (Year Over Year) end date      
  ly_sos_start_date   date NOT NULL,    -- LY SoS (Semester Over Semester) start date             
  ly_sos_end_date     date NOT NULL,    -- LY SoS (Semester Over Semester) end date             
  ly_qoq_start_date   date NOT NULL,    -- LY QoQ (Quarter Over Quarter) start date            
  ly_qoq_end_date     date NOT NULL,    -- LY QoQ (Quarter Over Quarter) end date            
  ly_mom_start_date   date NOT NULL,    -- LY MoM (Month Over Month) start date            
  ly_mom_end_date     date NOT NULL,    -- LY MoM (Month Over Month) end date            
  ly_wow_start_date   date NOT NULL,    -- LY WoW (Week Over Week) start date            
  ly_wow_end_date     date NOT NULL,    -- LY WoW (Week Over Week) end date            

  -- Australia fiscal calendar - year starts from 1 July and ends the next year on 30 June
  au_fiscal_year            smallint NOT NULL,
  au_fiscal_year_start_date date NOT NULL,        -- Fiscal YTD
  au_fiscal_year_end_date   date NOT NULL,
  au_fiscal_year_seq        smallint NOT NULL,    -- Sequence of numbers by fiscal year, begining from calendar start date till end.  Eg 1970-01-01 till 2050-01-01
  
  au_fiscal_month     smallint NOT NULL,    -- Fiscal MTD
  au_key_fiscal_month char(6) NOT NULL,     -- Fiscal year with month. Ex: 202101
  au_fiscal_month_seq smallint NOT NULL,    -- Sequence of numbers by fiscal month, begining from calendar start date till end.  Eg 1970-01-01 till 2050-01-01.


  au_fiscal_quarter           smallint NOT NULL,  -- Quarter number 1,2,3,4       
  au_fiscal_quarter_abbrv     char(2) NOT NULL,   -- Quarter abbreviation Q1, Q2, Q3, Q4  
  au_key_fiscal_long_quarter  char(6) NOT NULL,   -- Fiscal year with quarter. Ex: 2021Q1
  au_fiscal_quarter_seq       smallint NOT NULL,  -- Sequence of numbers by fiscal quarter, begining from calendar start date till end.  Eg 1970-01-01 till 2050-01-01
  au_fiscal_quarter_start_date date NOT NULL,
  au_fiscal_quarter_end_date  date NOT NULL,   
  au_fiscal_day_of_quarter    smallint NOT NULL,  -- 1,2,3 ... 90 till quarter end
  au_fiscal_month_of_quarter  smallint NOT NULL,  -- Month from 1 to 3 for each quarter.

  au_fiscal_day_of_year       smallint NOT NULL,  -- 1..365,366
  au_business_day_seq         smallint,           -- Number from 1 to total number of records in the date dimension table.
                                                  -- The number is only applicable for weekdays and non-public AU holidays

  -- USA fiscal federal calendar - year starts from 1 October and ends the next year on 30 September
  us_fiscal_year            smallint NOT NULL,
  us_fiscal_year_start_date date NOT NULL,
  us_fiscal_year_end_date   date NOT NULL,
  us_fiscal_year_seq        smallint NOT NULL,  -- Sequence of numbers by fiscal year, begining from calendar start date till end.  Eg 1970-01-01 till 2050-01-01
  
  us_fiscal_month     smallint NOT NULL,
  us_fiscal_month_seq smallint NOT NULL,  -- Sequence of numbers by fiscal month, begining from calendar start date till end.  Eg 1970-01-01 till 2050-01-01
  us_key_fiscal_month char(6) NOT NULL,   -- Fiscal year with quarter. Ex: 20211 (Fiscalyear + FiscalMonth)

  us_fiscal_quarter             smallint NOT NULL, 
  us_key_fiscal_quarter         char(2) NOT NULL,   -- Quarter abbreviation Q1, Q2, Q3, Q4  
  us_key_fiscal_quarter_long    char(6) NOT NULL,   -- Fiscal year with quarter. Ex: 2021Q1
  us_fiscal_quarter_seq         smallint NOT NULL,  -- Sequence of numbers by fiscal quarter, begining from calendar start date till end.  Eg 1970-01-01 till 2050-01-01
  us_fiscal_quarter_start_date  date NOT NULL,  
  us_fiscal_quarter_end_date    date NOT NULL,   
  us_fiscal_day_of_quarter      smallint NOT NULL,  -- 1,2,3 ... 90 till quarter end
  us_fiscal_month_of_quarter    smallint NOT NULL,  -- Month from 1 to 3 for each quarter.

  us_fiscal_day_of_year         smallint NOT NULL,  -- 1..365,366
  us_business_day_seq           smallint,           -- Number from 1 to total number of records in the date dimension table.
                                                    -- The number is only applicable for weekdays and non-public US holidays

  -- Seasons
  au_season_name varchar(25),   -- AU  - Summer, Autumn, Winter, Spring
  us_season_name varchar(25),   -- USA - Summer, Autumn\Fall, Winter, Spring

  -- Holidays
  au_is_holiday   char(1) NOT NULL, -- Y/N
  au_holiday_name varchar(50),      -- AU Holiday name

  us_is_holiday   char(1) NOT NULL,   -- Y/N  
  us_holiday_name varchar(50),        -- US Holiday name

  -- Date formats
  date_usa_format char(10) NOT NULL,    -- mm/dd/yyyy
  date_uk_format  char(10) NOT NULL,    -- dd/mm/yyyy
  date_iso_format char(10) NOT NULL,    -- yyyy-mm-dd
  date_expanded   char(50) NOT NULL,    -- 1st January 2020

  PRIMARY KEY (id_seq)
  ) diststyle ALL encode auto;


INSERT INTO public.dim_date
WITH recursive numbers(n) as
( 
  SELECT 1 as n
    UNION all
    SELECT n + 1
    FROM numbers n
    WHERE n.n <= datediff(days,'1970-01-01'::date - interval '1 day',last_day('2069-12-31'))
),
date_list as (
	SELECT n as id,trunc(('1970-01-01'::date - interval '1 day') + n * interval '1 day') as cal_date FROM numbers
  )
SELECT  
    id as id_seq, 						         -- Sequential numbers from 1 to count of total records for the date range
    cal_date as cal_date ,             -- Date value

    -- Date in epoch. Technically the integer overflow will occur on 19th Jan 2038.
    -- Workaround is to manually compute by adding 86400 seconds, times the number of days from Jan 1, 1970 after this date.
    -- https://en.wikipedia.org/wiki/Year_2038_problem
    case 
      when cal_date < '2038-01-18'::date then extract(epoch from cal_date )
    else datediff(day,'1970-01-01'::date, cal_date ) * 86400
    end as date_epoch,   -- Unix epoch from date



    --===========================================================================
    -- Day
    extract(dow from cal_date )  as day_of_week,     	  -- Day of the week - 0 (Sunday) to 6 (Saturday)
    to_char(cal_date , 'Day')    as day_name_of_week,	  -- DayName - Sunday, Monday, Tuesday .. Saturday       
    extract(day from cal_date )  as day_of_month,  	    -- Get day of the month 1..28,29,30,31
    datediff(day,date_trunc('quarter', cal_date )::date,cal_date )+1 as day_of_quarter, -- Day of quarter. From 1..90,91,92
    extract(doy from cal_date )  as day_of_year,  	    -- Day of the year 1..366

    -- Is WeekDay will be 'N' when  day_of_week is either 0 (Saturday) or 6 (Sunday), else 'Y'
    case 
      when day_of_week in (0,6) 
           then 'N' else 'Y' 
    end as is_weekday,	  

    -- Map the day of month to 'st,nd,rd,th'. 
    case  
         when day_of_month = 1 then 'st'
         when day_of_month = 21 then 'st'
         when day_of_month = 31 then 'st'
         
         when day_of_month = 2 then 'nd'
         when day_of_month = 22 then 'nd'

         when day_of_month = 3 then 'rd'
         when day_of_month = 23 then 'rd'

         when day_of_month is null then null  -- For nulls return NULLs, though this is not required here; checking it as a practice.
        else 'th'
    end as day_of_month_suffix,

    dateadd(day, -1, cal_date ) as dod ,      -- Get previous day (Day Over Day - DoD)
    0 as business_day_seq,                  -- Populated later on with dense rank and criteria

    --===========================================================================
    --  Week
    -- https://docs.aws.amazon.com/redshift/latest/dg/r_FORMAT_strings.html
    to_char(cal_date ,'WW')::smallint as week_of_year,     -- Week of the Year starting from 1-52, counting from first day of the year
    to_char(cal_date ,'IW')::smallint as week_of_year_iso, -- Week of the Year as per ISO 8601. where Monday is the first day of the week
    to_char(cal_date ,'W')::smallint as week_of_month,     -- Week of the month. Every seven day is counted as week and goes from 1 to 5
    date_trunc('week',cal_date ) AS week_start_date ,       -- Week start date from Monday
    (week_start_date + interval '6 day')::date as week_end_date , -- Week end date from Sunday

    -- Week Over Week (WoW) get week before current date.
    -- Example: Current date: 2022-01-01 WeekStartDate: 2021-12-27 WeekEndDate:2022-01-02
    -- PriorWeekStartDate: 2021-12-20 PriorWeekEndDate: 2021-12-26
    date_trunc('week', cal_date - interval '6 day')::date as wow_start_date ,
    (wow_start_date + interval '6 day')::date as wow_end_date ,

    case  -- Map the week of month to 'st,nd,rd,th'. 
         when week_of_month = 1 then 'st'
         when week_of_month = 2 then 'nd'
         when week_of_month = 3 then 'rd'
         when week_of_month is null then null  -- For nulls return NULLs, though this is not required here; checking it as a practice.
        else 'th'
    end as week_of_month_suffix,

    case  -- Map the week of year to 'st,nd,rd,th'. 
         when week_of_year = 1  then 'st'
         when week_of_year = 21 then 'st'
         when week_of_year = 31 then 'st'
         when week_of_year = 41 then 'st'
         when week_of_year = 51 then 'st'
         
         when week_of_year = 2  then 'nd'
         when week_of_year = 22 then 'nd'
         when week_of_year = 32 then 'nd'
         when week_of_year = 42 then 'nd'
         when week_of_year = 52 then 'nd'

         when week_of_year = 3  then 'rd'
         when week_of_year = 23 then 'rd'         
         when week_of_year = 33 then 'rd'
         when week_of_year = 53 then 'rd'         

         when week_of_year is null then null  -- For nulls return NULLs, though this is not required here; checking it as a practice.
        else 'th'
    end as week_of_year_suffix,

    case  -- Map the week of year to 'st,nd,rd,th'. 
         when week_of_year_iso = 1  then 'st'
         when week_of_year_iso = 21 then 'st'
         when week_of_year_iso = 31 then 'st'
         when week_of_year_iso = 41 then 'st'
         when week_of_year_iso = 51 then 'st'
         
         when week_of_year_iso = 2  then 'nd'
         when week_of_year_iso = 22 then 'nd'
         when week_of_year_iso = 32 then 'nd'
         when week_of_year_iso = 42 then 'nd'
         when week_of_year_iso = 52 then 'nd'

         when week_of_year_iso = 3  then 'rd'
         when week_of_year_iso = 23 then 'rd'         
         when week_of_year_iso = 33 then 'rd'
         when week_of_year_iso = 53 then 'rd'

         when week_of_year_iso is null then null  -- For nulls return NULLs, though this is not required here; checking it as a practice.
        else 'th'
    end as week_of_year_iso_suffix,
    -1 as week_seq,                             -- Updated later on with dense_rank
    -1 as week_iso_seq,                         -- Updated later on with dense_rank



    --===========================================================================
    -- Month
    extract(month from cal_date ) as month, 		-- Month number from date. 1,2...12
    
    case  -- Month of the year suffix 'st,nd,rd,th'. 
         when month = 1 then 'st'
         when id = 2 then 'nd'
         when id = 3 then 'rd'
         when id is null then null  -- For nulls return NULLs, though this is not required here; checking it as a practice.
        else 'th'
    end as month_suffix,    

    to_char(cal_date , 'Month') as month_name,		        -- Month name from date. January, February, March ... December
    to_char(cal_date , 'Mon') as month_name_abbrv,	      -- Month name abbreviated. Eg: Jan, Feb, Nov, Dec
    date_trunc('month',cal_date )::date as month_start_date , -- Start date of the month Ex. 2021-12-01 (Use case MTD)
    last_day(cal_date ) as month_end_date ,           -- Last date of the month. Ex. 2021-12-31

    date_trunc('month',(dateadd(month, -1, cal_date )))::date as mom_start_date , -- Month Over Month - prior month start date 
    last_day(mom_start_date ) as mom_end_date ,     -- Month Over Month - prior month end date
    -1 as month_seq,                                -- Updated later on with dense_rank

    -- Substract current month number from quarter start date month value. 
    -- The value one is added to start numbering from one instead of zero
    (month - extract(month from date_trunc('quarter', cal_date )::date))+1  as month_of_quarter,



    --===========================================================================
    -- Quarter
    extract(qtr from cal_date ) as quarter, 	-- Calendar quarter 1,2,3,4

    -- Example starting date of 1st quarter 2020-01-01
    date_trunc('quarter', cal_date )::date AS quarter_start_date , 

    -- Example ending date of first quarter 2020-03-31
    (dateadd(month,3,quarter_start_date ) - interval '1 day')::date AS quarter_end_date, 

    case
        when quarter = 1 then 'Q1'
        when quarter = 2 then 'Q2'
        when quarter = 3 then 'Q3'
        when quarter = 4 then 'Q4'
    end as quarter_abbrv, -- Quarter abbreviation Q1, Q2, Q3, Q4

    date_trunc('quarter', quarter_start_date - interval '1 day')::date as qoq_start_date ,  -- Previous quarter start date
    (dateadd(month, 3, qoq_start_date ) - interval '1 day')::date as qoq_end_date ,         -- Previous quarter end date
    -1 as quarter_seq, -- Updated later on with dense_rank



    --===========================================================================
    -- Semester

    -- Semster ID for the year 1,2
    ceil(month/6::decimal) as semester_id,               

    -- Sem1 start date for the year Ex. 2020-01-01 (Semster To Date).
    dateadd(month, (1%semester_id * 6)::integer, date_trunc('year', cal_date ))::date as  semester_start_date ,

    -- Sem1 end date for the year Ex. 2020-06-30
    (dateadd(month, 6, semester_start_date ) - interval '1 day')::date as semester_end_date ,

    -- Semester Over Semester start date (Previous semester start date)
    dateadd(month, 1 * (-6), semester_start_date )::date  as sos_start_year ,

    -- Semester Over Semester end date (Previous semester end date)
    (dateadd(month, 6, sos_start_year ) - interval '1 day')::date as sos_end_year ,

    -- Days from semester start date till current date (STD - Semester Till Date)
    datediff(day,semester_start_date ,cal_date ) + 1 as day_of_semester,
    
    -- Semester Abbrev. Values: S1,S2
    ('S' || semester_id)::text as semester_abbrv,

    -1 as semester_seq, -- Updated later on with dense_rank

    --===========================================================================
    -- Year
    extract(year from cal_date ) as year,  		          -- YEAR from date. Example For calenday year 2000, the value is 2000
    to_char(cal_date ,'YY')::smallint as year_short,     -- Year with last 2 digits like 22 instead of 2022
    to_char(cal_date ,'IYYY')::smallint as year_iso,   	-- ISO Year from date. Example For calenday year 2000, the value is 1999
    date_trunc('year', cal_date )::date as year_first_day ,

    -- Add eleven months to start year date and get last date of the month
    last_day(dateadd(month,11,year_first_day )) AS year_last_day ,

    -- Logic for IsLeapYear (Year mod 4 = 0) AND ((Year mod 100 != 0) or (Year mod 400 = 0)))
    case 
      when year%4=0 and (year % 100 != 0 or year % 400 = 0) then 'Y' 
      else 'N'
    end as is_leap_year,

    -1 as year_seq, -- Updated later on with dense_rank



    --===========================================================================
    -- Date Keys    
    to_char(cal_date ,'YYYYMMDD')    as key_date,                -- 20210231 (YYYYMMDD)
    to_char(cal_date ,'YYMMDD')      as key_short_date,          -- 210231 (YYMMDD)
    to_char(cal_date ,'YYYYMM')      as key_month,               -- 202101 (YYYYMM)
    to_char(cal_date ,'YYMM')        as key_short_month,         -- 2101 (YYMM)    
    (year || quarter_abbrv)          as key_qaurter,             -- 2021Q1 (YYYYQQ)
    (year_short || quarter_abbrv)    as key_short_qaurter,       -- 21Q1   (YYQQ)
    (year || semester_abbrv)         as key_semester,            -- 2021S1
    (year_short || semester_abbrv)   as key_short_semester,      -- 21S1
    to_char(cal_date ,'YYYYWW')      as key_week_year,           -- 202153 (YYYYWW)
    to_char(cal_date ,'YYWW')        as key_short_week_year,     -- 2153 (YYWW)
    to_char(cal_date ,'YYYYIW')      as key_week_iso_year,       -- 202152 (YYYYIW)
    to_char(cal_date ,'YYIW')        as key_short_week_iso_year, -- 2152 (YYIW)



    --===========================================================================
    -- Prior X days from current date

    dateadd(day, -360, cal_date)::date as prior_360d_from_date ,    -- Current date - 360 days
    dateadd(day, -180, cal_date)::date as prior_180d_from_date ,    -- Current date - 180 days
    dateadd(day, -120, cal_date)::date as prior_120d_from_date,     -- Current date - 120 days
    dateadd(day, -90,  cal_date)::date as prior_90d_from_date,      -- Current date - 90 days
    dateadd(day, -60,  cal_date)::date as prior_60d_from_date,      -- Current date - 60 days
    dateadd(day, -30,  cal_date)::date as prior_30d_from_date,      -- Current date - 30 days
    dateadd(day, -14,  cal_date)::date as prior_14d_from_date,      -- Current date - 14 days
    dateadd(day, -7,   cal_date)::date as prior_7d_from_date,       -- Current date - 7 days



    --===========================================================================
    -- LastYear YTD, STD (Semester To Date), QTD, MTD, WTD.
    -- If the current date is 3rd March 2020, then LastYear YTD start will be 1 Jan 2019, and end date 3rd March 2019.

    -- Get current year start date and substract 1 year, to get prior year start date
    dateadd(year, -1, year_first_day)::date as ly_ytd_start_date,
    -- Get the current day of the year, and add same number of days to prior year.
    dateadd(day, (day_of_year-1), ly_ytd_start_date)::date as ly_ytd_end_date, 

    dateadd(year, -1, semester_start_date) as ly_std_start_date,
    dateadd(day, day_of_semester, ly_std_start_date)::date as ly_std_end_date,
    
    date_trunc('quarter', dateadd(year,-1,cal_date ))::date as ly_qtd_start_date,
    dateadd(day, day_of_quarter - 1, ly_qtd_start_date)::date as ly_qtd_end_date,
    
    dateadd(year,-1, month_start_date)::date as ly_mtd_start_date,

    -- ly_mtd_end_date will give same number of days for the same month last year. Only applicable when prior year is leap year.
    -- For instance if the current date is 2008-02-29 (leap year) with 29 days in Feb.
    -- ly_mtd_end_date returns '2007-02-28' instead of '2003-03-01'. This exactly matches the number of days   
    dateadd(day, (day_of_month -1), ly_mtd_start_date)::date as ly_mtd_end_date,

    -- ly_mtd_end_date will NOT give same number of days for the same month last year. Only applicable when prior year is leap year.
    -- For instance if the current date is 2008-02-29 (leap year) with 29 days in Feb.
    -- ly_mtd_end_date_actual returns '2007-03-01', even though the current date is 2008-02-29  
    last_day(dateadd(day, (day_of_month -1), ly_mtd_start_date))::date as ly_mtd_end_date_actual,    
    
    
    -- Get the same week number for the previous year
    date_trunc('week',dateadd(year,-1,cal_date )) AS ly_wtd_start_date,    -- Week start date from Monday
    ly_wtd_start_date + interval '6 day' as ly_wtd_end_date,



    --===========================================================================
    -- Last Year YoY, SoS, QoQ, MoM, WoW (See what happened during the same time frame last year)

    dateadd(year, -1, year_first_day)::date       as  ly_yoy_start_date,      -- LY YoY (Year Over Year) start date
    dateadd(year, -1, year_last_day)::date        as  ly_yoy_end_date,        -- LY YoY (Year Over Year) end date      
    dateadd(year, -1, semester_start_date)::date  as  ly_sos_start_date,      -- LY SoS (Semester Over Semester) start date             
    dateadd(year, -1, semester_end_date)::date    as  ly_sos_end_date,        -- LY SoS (Semester Over Semester) end date             
    dateadd(year, -1, quarter_start_date)::date   as  ly_qoq_start_date,      -- LY QoQ (Quarter Over Quarter) start date            
    dateadd(year, -1, quarter_end_date)::date     as  ly_qoq_end_date,        -- LY QoQ (Quarter Over Quarter) end date            
    dateadd(year, -1, month_start_date)::date     as  ly_mom_start_date,      -- LY MoM (Month Over Month) start date            
    dateadd(year, -1, month_end_date)::date       as  ly_mom_end_date,        -- LY MoM (Month Over Month) end date            
    dateadd(year, -1, week_start_date)::date      as  ly_wow_start_date,      -- LY WoW (Week Over Week) start date            
    dateadd(year, -1, week_end_date)::date        as  ly_wow_end_date,        -- LY WoW (Week Over Week) end date   



    --===========================================================================
    -- Australia - Fiscal Year & Quarter  - Starts from 1st July and ends on following year 30th June

    -- Set AU fiscal year, if current month is on or after July, then set fiscal year to current year, else prior year
    case when month >= 7 then year else year - 1 end as au_fiscal_year,

    -- Get AU fiscal start date, by appending au fiscal year to 1 July
    to_date(au_fiscal_year || '-07-01', 'YYYY-MM-DD') as au_fiscal_year_start_date,

    -- Get fiscal AU year end date by, adding 11 months to fiscal start date, and get last date of the month
    last_day(dateadd(month, 11, au_fiscal_year_start_date)) as au_fiscal_year_end_date,

    -1 as au_fiscal_year_seq, -- Updated later on with dense_rank

    -- Get fiscal month by substracting current month from fiscal year start date
    case when cal_date between au_fiscal_year_start_date and au_fiscal_year_end_date 
            then datediff(month, au_fiscal_year_start_date, cal_date ) + 1
    end as au_fiscal_month,
    
    -- Pad zeros before month. Eg: For the month of January convert month number 1 to 01
    au_fiscal_year || lpad(au_fiscal_month::text,2,'0') as au_key_fiscal_month, -- 202101

    -1 as au_fiscal_month_seq, -- Updated later on with dense_rank

    -- Get AU fiscal quarter by dividing the AU fiscal month by 3 and getting the ciel (next whole number).
    -- Values  1,2,3,4
    ceil(au_fiscal_month/3::decimal) as au_fiscal_quarter,
    
    case 
        when au_fiscal_quarter = 1 then 'Q1'
        when au_fiscal_quarter = 2 then 'Q2'
        when au_fiscal_quarter = 3 then 'Q3'
        when au_fiscal_quarter = 4 then 'Q4'
    end as au_fiscal_quarter_abbrv, 

    -- Fiscal year with quarter. Ex: 2021Q1
    au_fiscal_year::text || au_fiscal_quarter_abbrv as au_key_fiscal_long_quarter,

    -1 as au_fiscal_quarter_seq, -- Updated later on with dense_rank

    -- Get AU quarter start date, if quarter is 1 then get AU fiscal year start date else, add quater value to the fiascal year start date for AU
    case 
      when au_fiscal_quarter = 1 then au_fiscal_year_start_date
      else dateadd(quarter, au_fiscal_quarter::integer - 1, au_fiscal_year_start_date)::date 
    end as au_fiscal_quarter_start_date,

    -- Get quarter end date, by adding two months to AU fiscal quarter start date
    last_day(dateadd(month, 2, au_fiscal_quarter_start_date)) as au_fiscal_quarter_end_date,
    
    -- Get day count in AU fiscal quarter by substracting days from AU quarter start date
    datediff(day, au_fiscal_quarter_start_date, cal_date )+1 as au_fiscal_day_of_quarter,

    -- Substract current month number from AU quarter start date month value. 
    -- The value one is added to start numbering from one instead of zero
    (month - extract(month from au_fiscal_quarter_start_date))+1  as au_fiscal_month_of_quarter,

    -- Get day count in AU fiscal year by substracting days from AU fiscal year start date
    datediff(day, au_fiscal_year_start_date, cal_date )+1 as au_fiscal_day_of_year,

    0 as au_business_day_seq,    -- Populated later on with dense rank and criteria

    --===========================================================================
    -- Fiscal federal Year & Quarter USA  - Starts from 1st October and ends on following year 30th September

    -- Set USA fiscal year, if current month is on or after July, then set fiscal year to current year, else prior year
    case when month >= 10 then year else year - 1 end as us_fiscal_year,

    -- Get USA fiscal start date, by appending USA fiscal year to 1 July
    to_date(us_fiscal_year || '-10-01', 'YYYY-MM-DD') as us_fiscal_year_start_date,

    -- Get fiscal USA year end date by, adding 11 months to fiscal start date, and get last date of the month
    last_day(dateadd(month, 11, us_fiscal_year_start_date)) as us_fiscal_year_end_date,

    -1 as us_fiscal_year_seq, -- Updated later on with dense_rank

    -- Get fiscal month by substracting current month from fiscal year start date
    case 
      when cal_date between us_fiscal_year_start_date and us_fiscal_year_end_date 
            then datediff(month, us_fiscal_year_start_date, cal_date ) + 1
    end as us_fiscal_month,
    
    -1 as us_fiscal_month_seq, -- Updated later on with dense_rank

    -- Pad zeros before month. Eg: For the month of January convert month number 1 to 01
    us_fiscal_year || lpad(us_fiscal_month::text,2,'0') as us_key_fiscal_month, -- 202101

    -- Get USA fiscal quartner by dividing the USA fiscal month by 3 and getting the ciel (next whole number).
    ceil(us_fiscal_month/3::decimal) as us_fiscal_quarter,
    
    case 
        when us_fiscal_quarter = 1 then 'Q1'
        when us_fiscal_quarter = 2 then 'Q2'
        when us_fiscal_quarter = 3 then 'Q3'
        when us_fiscal_quarter = 4 then 'Q4'
    end as us_key_fiscal_quarter,

    -- Fiscal year with quarter. Ex: 2021Q1
    us_fiscal_year::text || us_key_fiscal_quarter as us_key_fiscal_quarter_long,

    -1 as us_fiscal_quarter_seq, -- Updated later on with dense_rank

    -- Get USA quarter start date, if quarter is 1 then get USA fiscal year start date else, add quater value to the fiascal year start date for USA
    case 
        when us_fiscal_quarter = 1 then us_fiscal_year_start_date
        else dateadd(quarter, us_fiscal_quarter::integer - 1, us_fiscal_year_start_date)::date 
    end as us_fiscal_quarter_start_date,

    -- Get quarter end date, by adding two months to USA fiscal quarter start date
    last_day(dateadd(month, 2, us_fiscal_quarter_start_date)) as us_fiscal_quarter_end_date,
    
    -- Get day count in USA fiscal quarter by substracting days from USA quarter start date   
    datediff(day, us_fiscal_quarter_start_date, cal_date )+1 as us_fiscal_day_of_quarter,

    -- Substract current month number from AU quarter start date month value. 
    -- The value one is added to start numbering from one instead of zero
    (month - extract(month from us_fiscal_quarter_start_date))+1  as us_fiscal_month_of_quarter,

    -- Get day count in USA fiscal year by substracting days from USA fiscal year start date
    datediff(day, us_fiscal_year_start_date, cal_date )+1 as us_fiscal_day_of_year,

    0 as us_business_day_seq,    -- Populated later on with dense rank and criteria

  --===========================================================================
    -- Seasons
    
    case -- AU Seasons
      when month = 12 or  month between 1 and 2 then 'Summer'
      when month between 12 and 2 then 'Summer'
      when month between 3  and 5 then 'Autumn'
      when month between 6  and 8 then 'Winter'
      when month between 9 and 11 then 'Spring'
      else null
    end as au_season_name,             -- Summer, Autumn, Winter, Spring


    case -- USA Seasons
      when month between 6  and 8  then 'Summer'
      when month between 9  and 11 then 'Fall'    --Autumn
      when month = 12 or month between 1 and 2  then 'Winter'
      when month between 3  and 5  then 'Spring'
      else null
    end as us_season_name,             -- Summer, Autumn\Fall, Winter, Spring


    --===========================================================================
    -- Holidays
    
    -- By default set to N for holidays, will be updated after loading holiday names from external file
    'N' as au_is_holiday,

    -- Public Holidays AU, loaded from a separate file
    null as au_holiday_name,

    -- By default set to N for holidays, will be updated after loading holiday names from external file
    'N' as us_is_holiday,

    -- Public Holidays USA, loaded from a separate file
    null as us_holiday_name,

    --===========================================================================
    -- Date Formats
    to_char(cal_date ,'MM/DD/YYYY') as date_usa_format,
    to_char(cal_date ,'DD/MM/YYYY') as date_uk_format,
    to_char(cal_date ,'YYYY-MM-DD') as date_iso_format, --ISO 8601 date format

    -- Sample Value: 1st January 2020
    day_of_month::text || day_of_month_suffix::text || ' '  || month_name || ' ' || year::text as date_expanded


FROM 
	date_list;


--===========================================================================
-- Update Sequence fields for 
-- Calendar year/semester/quarter/month/week 
-- Fiscal year 

with seq as (
  select cal_date ,
  		 key_week_year,
  		 dense_rank() over(order by key_week_year) as week_seq_val,
       dense_rank() over(order by key_week_iso_year) as week_iso_seq_val,
  		 dense_rank() over(order by key_month) 	as month_seq_val,
  		 dense_rank() over(order by key_qaurter) 	as quarter_seq_val,
  		 dense_rank() over(order by key_semester) as semester_seq_val,
  		 dense_rank() over(order by year) 		as year_seq_val,
  
  		 dense_rank() over(order by au_fiscal_year) 	as au_fiscal_year_seq_val,
  		 dense_rank() over(order by us_fiscal_year) 	as us_fiscal_year_seq_val
  from
  	dim_date
  )
update dim_date set 
	  week_seq = week_seq_val,
    week_iso_seq = week_iso_seq_val,
    month_seq = month_seq_val,
    quarter_seq = quarter_seq_val,
    semester_seq = semester_seq_val,
    year_seq = year_seq_val,
    
    au_fiscal_year_seq = au_fiscal_year_seq_val,    
    us_fiscal_year_seq = us_fiscal_year_seq_val
    
from seq where seq.cal_date = dim_date.cal_date ;

--===========================================================================
-- Update business day sequence for general calendar
with seq as (
  select cal_date ,  		 
  		 dense_rank() over(order by cal_date) as business_day_seq_val
  from
  	dim_date where is_weekday='Y'  
  )
update dim_date set 
	  business_day_seq = business_day_seq_val    
from seq where seq.cal_date = dim_date.cal_date ;


--===========================================================================
-- Update field au_fiscal_month_seq. 
-- The au_fiscal_month_seq must correspond to fiscal month number, 
-- and should increment till the last record in the date dimension table

with t_au_fiscal_start_date as ( 
    -- Get the very fist au_fiscal_year_start_date in the table   
    select au_fiscal_year_start_date from dim_date where id_seq = 1
  ),
au_fiscal_month_logic as ( 
    --  Join t_au_fiscal_start_date with every record in dim_date  
    select 
      id_seq,
      -- Substract months from the au_fiscal_year_start_date in the very first record
      datediff(month, t.au_fiscal_year_start_date, cal_date ) + 1 new_au_fiscal_month 
  from 
    t_au_fiscal_start_date t,
      dim_date dd
  order by dd.cal_date
  )
update 
  dim_date 
  SET au_fiscal_month_seq = t.new_au_fiscal_month 
  from au_fiscal_month_logic t 
  where dim_date.id_seq = t.id_seq;

--===========================================================================
-- Update field us_fiscal_month_seq. 
-- The us_fiscal_month_seq must correspond to fiscal month number, 
-- and must increment till the last record in the date dimension table

with t_us_fiscal_start_date as ( 
    -- Get the very fist us_fiscal_year_start_date in the table   
    select us_fiscal_year_start_date from dim_date where id_seq = 1
  ),
us_fiscal_month_logic as ( 
    --  Join t_us_fiscal_start_date with every record in dim_date  
    select 
      id_seq,
      -- Substract months from the us_fiscal_year_start_date in the very first record
      (month - extract(month from t.us_fiscal_year_start_date))+1 new_us_fiscal_month 
  from 
    t_us_fiscal_start_date t,
      dim_date dd
  order by dd.cal_date
  )
update 
  dim_date 
  SET us_fiscal_month_seq = t.new_us_fiscal_month 
  from us_fiscal_month_logic t 
  where dim_date.id_seq = t.id_seq;
    

--===========================================================================
-- Update field au_fiscal_quarter_seq. 
-- The au_fiscal_quarter_seq must correspond to fiscal month number, 
-- and must increment till the last record in the date dimension table

with 
au_fiscal_quarter_logic as ( 
    select 
      id_seq,
      -- Divide au_fiscal_month_seq by 3 and round off next whole number
      ceil(au_fiscal_month_seq/3::decimal) new_au_fiscal_quarter
  from 
      dim_date
  order by cal_date
  )
update 
  dim_date 
  SET au_fiscal_quarter_seq = t.new_au_fiscal_quarter
  from au_fiscal_quarter_logic t 
  where dim_date.id_seq = t.id_seq;
    
--===========================================================================
-- Update field us_fiscal_quarter_seq. 
-- The us_fiscal_quarter_seq must correspond to fiscal month number, 
-- and must increment till the last record in the date dimension table

with 
us_fiscal_quarter_logic as ( 
    select 
      id_seq,
      -- Divide us_fiscal_month_seq by 3 and round off next whole number
      ceil(us_fiscal_month_seq/3::decimal) new_us_fiscal_quarter
  from 
      dim_date
  order by cal_date
  )
update 
  dim_date 
  SET us_fiscal_quarter_seq = t.new_us_fiscal_quarter
  from us_fiscal_quarter_logic t 
  where dim_date.id_seq = t.id_seq;



COMMIT TRANSACTION;
