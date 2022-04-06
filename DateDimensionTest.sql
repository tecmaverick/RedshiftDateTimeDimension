-- View Juneteenth holidays falling on Saturday and Sunday and is on 19th June
with records_both_in_saturday_and_sunday as 
(
    select count(cal_date_d) from dim_date dd 
    where 
        dd.holiday_usa_name_t = 'Juneteenth National Independence Day' and 
        dd.month_n = 6 and 
        dd.day_of_month_n = 19 and 
        dd.day_of_week_n in (6,0)
),
records_moved_to_friday_and_monday as (
--  This must tally with the above count
select count(cal_date_d) from dim_date dd 
where 
    dd.holiday_usa_name_t = 'Juneteenth National Independence Day (Observance)' 
)
select 
    case when count(a.*)=count(b.*) then 'Pass'
    else 'Fail'
    end as JuneteenthRecordTally
from 
    records_both_in_saturday_and_sunday a, records_moved_to_friday_and_monday b

-- ------------------------------------------------------------------------------------
--   View the seq values
  select cal_date_d,
  		  key_week_year_t, week_seq_n,
  		  key_month_t, month_seq_n,
  		  key_qaurter_t, quarter_seq_n,
  		  key_semester_t, semester_seq_n,
  		  year_n, year_seq_n,
  
  		  au_fiscal_year_n, au_fiscal_year_seq_n,
  		  au_fiscal_month_abbrv_t, au_fiscal_month_seq_n,  
  		  au_fiscal_long_quarter_abbrv_t, au_fiscal_quarter_seq_n, 

  		  us_fiscal_year_n, us_fiscal_year_seq_n,
  		  us_fiscal_month_abbrv_t, us_fiscal_month_seq_n,  
  		  us_fiscal_long_quarter_abbrv_t, us_fiscal_quarter_seq_n
  from
  	dim_date order by cal_date_d;
 

-- Verify the Epoch values are 86400 seconds apart
with epochtest as (
    select 
        date_d,
        date_epoch_n,
        lag(date_epoch_n) over (order by date_d) as prev_date_epoch_m,
        case 
            when prev_date_epoch_m +86400 = date_epoch_n  then 'T'
            else 'F'
        end as status
        from dim_date order by date_d
        )
select * from epochtest where status='F';

-- Get number of records in dim_date table
select count(date_d)as record_count from dim_date;

-- Get number of fields in dim_date table
select * from PG_TABLE_DEF where tablename ='dim_date' ;

-- View all records in dim_date
select * from dim_date;

-- View holidays
select cal_date_d,holiday_au_name_t,holiday_usa_name_t from dim_date order by cal_date_d;

