# The S3 bucket name where the Holidays.csv is stored
s3_bucket="replace_with_s3_bucket_name"
aws_cli_profile="default" # replace default with profile name, if using a non-default profile name

# Create a virtual environment, for the script to install dependencies
python3 -m venv .

# Activate the virtual environment
source ./bin/activate

# Install dependencies
pip install --no-cache-dir -r requirements.txt

# Run script to generate file 'Holidays.csv'
# By default, generates a holiday list from "1970-01-01" to "2069-12-31"
python3 main.py

# Deactivate virtual enviornment
deactivate

# Delete all installed dependencies
rm -rf ./lib ./include ./bin ./pyvenv.cfg

# Copy the Holidays.csv to S3 bucket
aws s3 cp Holidays.csv s3://$s3_bucket --profile=$aws_cli_profile

