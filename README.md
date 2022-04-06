# Redshift DateTime Dimension

The script creates Date Dimension table public.dim_date and populates it with 100 years of data starting from 1st January 1970 to 31st Dec 2069. Fiscal calendars for Australia and USA along with the federal holidays are loaded.


## Pre-requisite:
1. AWS Account
2. AWS CLI [1]
3. Redshift Cluster 
4. IAM Role attached to Redshift cluster with S3 bucket read permission [2]
5. IAM role attached to Redshift is set to default [3]
6. AWS account logged in user should have Read and Write access to an S3 bucket in the same region as Redshift cluster
7. Python3 - Required for generating holiday list


## Loading Dimension table
1. Execute contents of SQL file 'DimensionTables/DateDimension.sql' on redshift cluster

2. Geneate federal holiday file for Australia and USA. 
2.1 Update variable values in "PublicHolidays\GeneratePublicHolidayFile.sh" and execute the script. 
   The script generates the file Holidays.csv and uploads to S3 bucket.

3. Load holidays data from S3 bucket to redshift
3.1 Open "PublicHolidays\DateDimensionHoliday.sql" and update the S3 bucket name
3.2 Execute contents of SQL file 'DimensionTables/DateDimensionHoliday.sql' on redshift cluster

4. List the contents of the date dimension table
   ```
   SELECT * FROM PUBLIC.DIM_DATE ORDER BY CAL_DATE limit 1000;
   ```

## Fields Documentation
Please refer to the inline comments in DateDimension.sql

## FAQs

1. How are federal holidays calculated?
   All federal holidays are sourced from 'holidays' Python library  ( https://pypi.org/project/holidays/ ) 
   Those missing ones are manually loaded from DateDimensionHoliday.sql. 
   For US the following holidays were loaded separately - Juneteenth, Good Friday (for US), Easter Sunday, Martin Luther King Jr. Day

2. How is Easter date calculated?
   All easter dates are based on Western calendar, and is pulled from Python dateutil library ( https://dateutil.readthedocs.io/en/stable/easter.html )

3. Can I set any date range in the script?
   Yes you can. The script is tested for date range from 1st January 1970 to 31st December 2069. I recommend testing for date ranges outside these.

4. Is it possible to generate holiday list for other countries?
   Yes, as long as "Holidays" python library supports it. In "PublicHolidays/main.py" any supported country abbreviation can be added to "countries" list.

4. Why 
5. For the month of February, how the fields 'ly_mtd_start_date' and 'ly_mtd_end_date' are calculated when current year is leap year? 
   
   Current Date: 2008-02-29 (2008 is a leap year, that has 29 days in February). 
   'ly_mtd_start_date' will be '2007-02-01'
   'ly_mtd_end_date'   will be '2007-03-01' This is because prior year February had 29 days and current year only 28, which moves the date to next month.
      

6. For the month of February after leap year, how the fields 'ly_mtd_start_date' and 'ly_mtd_end_date' are calculated? 
   
   Current Date: 2009-02-28 (2009 is a year after leap year, which has only 28 days in February). 
   'ly_mtd_start_date' will be '2009-02-01'
   'ly_mtd_end_date'   will be '2007-02-29' 
   This may cause skew in reports comparing current month over prior year month, because of a day more.
   Use 'ly_mtd_end_date_actual' to exactly match the number of days as current month.


## Links
[1] AWS CLI - https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
[2] S3 Bucket Permissions - https://docs.aws.amazon.com/redshift/latest/mgmt/authorizing-redshift-service.html#authorizing-redshift-service-creating-an-iam-role
[3] Redshift Default IAM Role - https://docs.aws.amazon.com/redshift/latest/mgmt/default-iam-role.html#set-default-iam