-- ===================================================
-- Get SUM of orders by quarter, from 1990
-- for orders on weekdays, and on non-public holidays
-- and for first two weeks of the month
-- and first month of each fiscal quarter
-- and first quarter of each fiscal year
select 
     dd.au_key_fiscal_long_quarter as quarter, 
     SUM(o_totalprice ) as total_order_amt
from orders o   -- Orders fact table
join 
    dim_date dd -- Date dimension table
on 
    o.o_orderdate = dd.cal_date
where 
    dd.is_weekday ='Y' and -- Only on weekdays
    dd.au_is_holiday ='N' and -- Non public holidays
    dd.week_of_month in (1,2) and -- First two weeks of the month
    dd.au_fiscal_month_of_quarter = 1 and -- First month of each fiscal quarter
    dd.au_fiscal_quarter_abbrv = 'Q1' and -- Only for first fiscal quarter    
    dd.au_fiscal_year >= 1990 -- Starting from fiscal year 1990
group by 
    dd.au_key_fiscal_long_quarter  -- Contains value '1990Q1', '1990Q2'...
order by
    dd.au_key_fiscal_long_quarter