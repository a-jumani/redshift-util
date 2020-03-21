# Redshift Utility
---
This utility uses programmatic access to Amazon Redshift through `boto3` to automate simple, repetitive tasks. These currently include:
- Creating an S3 read-only role
- Starting a Redshift cluster given the configuration
- Making cluster allow incoming TCP connections to the specified port
- Terminating the cluster
- Checking status of the cluster

## Usage
Use `python manage_redshift.py -h` to display help.

## Setup
Python 3.7.6 was used to develop the utility. Use `pip install boto3` to install the needed module.
