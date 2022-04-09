"""
************************************************************************************************************
Pulls federal holidays for US and Australia from 1970-01-01 till 2069-12-31 (100 Years)
and creates file named "Holidays.csv" in the same directory as main.py

Author: Abraham
Created-on: 21th March 2022
Known Issues\Hacks: 
For USA, Good friday isn't a federal holiday, though financial institutions are closed on this date. Logic to handle this is included.
************************************************************************************************************

Change log:
Modified-on: 
Changes:
************************************************************************************************************
"""

from datetime import date, timedelta
from dateutil.easter import *
from dateutil.parser import *
import holidays


def date_range(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date.date() + timedelta(n)


def get_csv_header(countries):
    row_val = "date"
    for country_code in countries:
        row_val += "," + "{0}_holidays".format(country_code)

    return row_val + "\n"


def get_holiday_data(start_date, end_date, countries, flag_easter_and_good_friday_for_usa = True):
    """
    Generate the holidays for the given date range and count.
    For dates without holidays, no record will be present in csv file
    Parameters
        ----------
        start_date : datetime
            The start date from when the holiday list is to be generated
        end_date : datetime
            The end date till when the holiday list is to be generated            
        countries : list
            The list of countries for which the holiday list is to be generated
        flag_easter_and_good_friday_for_usa : bool, optional
            If Good friday is to be flagged as holiday set this flag

        Returns
        -------
        str
            A concatenated string of comma separated values
    """  

    data = get_csv_header(countries)
    country_holiday_cal = {}

    for country_code in countries:
        country_holiday_cal[country_code] = holidays.country_holidays(country_code)

    for current_date in date_range(start_date, end_date):
        easter_val = easter(current_date.year)
        date_val = current_date.strftime("%Y-%m-%d")
        holiday_names = {"exists": False}

        for country_code in countries:
            holiday_name = country_holiday_cal[country_code].get(current_date)

            if flag_easter_and_good_friday_for_usa and \
                    country_code.upper() == "US" and \
                    current_date == (easter_val - timedelta(days=2)) and holiday_name is None:
                holiday_name = "Good Friday"

            if flag_easter_and_good_friday_for_usa and \
                    country_code.upper() == "US" and \
                    easter_val == current_date:
                holiday_name = holiday_name or "Easter Sunday"

            if holiday_name:
                holiday_names["exists"] = True
                holiday_names[country_code] = holiday_name.replace(",","|")
            else:
                holiday_names[country_code] = ""

        # Logic to write holidays only if it exists for a date
        if holiday_names["exists"]:
            data += date_val
            for country_code in countries:
                data += "," + holiday_names[country_code]

            data += "\n"

    return data


if __name__ == '__main__':
    output_file_name = "Holidays.csv"
    start_date = parse("1970-01-01", yearfirst=True)
    end_date = parse("2069-12-31", yearfirst=True)

    # For list of country abbreviations
    # https://github.com/dr-prodigy/python-holidays
    countries = ['US', 'Australia']

    file_h = open(output_file_name, "w")
    file_h.write(get_holiday_data(start_date, end_date, countries))
    file_h.close()

    print(f"{output_file_name} generated")

