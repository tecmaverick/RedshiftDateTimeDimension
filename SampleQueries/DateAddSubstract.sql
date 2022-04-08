-- ===================================================
-- Substract 21 fiscal Australian quarter from current date,
--  and get the first date of that quarter
-- 'au_fiscal_quarter_seq' field holds sequence of numbers matching AU fiscal quarter

with current_au_quarter as (
	select cal_date,au_fiscal_quarter_seq, au_fiscal_quarter_start_date, au_fiscal_quarter_end_date
    from dim_date where cal_date = current_date
),
next_au_quarter as (
  	select d1.cal_date, d1.au_fiscal_quarter_seq, d1.au_fiscal_quarter_start_date, d1.au_fiscal_quarter_end_date
    from dim_date d1 inner join dim_date d2 on d1.cal_date = d2.au_fiscal_quarter_start_date
    where d1.au_fiscal_quarter_seq = (select au_fiscal_quarter_seq - 21 from current_au_quarter)
)
	select 'current' as label,cal_date,au_fiscal_quarter_seq,
            au_fiscal_quarter_start_date,
            au_fiscal_quarter_end_date from current_au_quarter
    union
    select 'prior' as label,cal_date,au_fiscal_quarter_seq,
            au_fiscal_quarter_start_date,
            au_fiscal_quarter_end_date from next_au_quarter


-- ===================================================
-- Add 15 business days (exclude weekdends) from current date
-- 'business_day_seq' field holds sequence of numbers excluding weekends

with current_biz_day as (
	select cal_date,business_day_seq from dim_date 
	where cal_date = current_date
),
next_biz_day as (
  select  cal_date,business_day_seq from dim_date  
  	where business_day_seq = (select business_day_seq + 15 from current_biz_day)
)
	select 'current' as label,cal_date,business_day_seq from current_biz_day
    union
    select 'next' as label,cal_date,business_day_seq from next_biz_day

-- ===================================================
-- Add 15 business days (exclude weekdends + Australia public holidays) from current date
-- 'au_business_day_seq' field holds sequence of numbers excluding weekends and AU public holidays

with current_biz_day as (
	select cal_date, au_business_day_seq from dim_date 
	where cal_date = current_date
),
next_biz_day as (
  select  cal_date, au_business_day_seq from dim_date  
  	where au_business_day_seq = (select au_business_day_seq + 15 from current_biz_day)
)
	select 'current' as label,cal_date, au_business_day_seq from current_biz_day
    union
    select 'next' as label,cal_date, au_business_day_seq from next_biz_day

-- ===================================================
-- Add 15 business days (exclude weekdends + US public holidays) from current date
-- 'us_business_day_seq' field holds sequence of numbers excluding weekends and US public holidays

with current_biz_day as (
	select cal_date, us_business_day_seq from dim_date 
	where cal_date = current_date
),
next_biz_day as (
  select  cal_date, us_business_day_seq from dim_date  
  	where us_business_day_seq = (select us_business_day_seq + 15 from current_biz_day)
)
	select 'current' as label,cal_date, us_business_day_seq from current_biz_day
    union
    select 'next' as label,cal_date, us_business_day_seq from next_biz_day
