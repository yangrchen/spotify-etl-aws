# Spotify ETL Experiments

This project serves as a way for me to learn how different AWS services interact together. Using the Spotify API, I built an ETL pipeline that extracts album and song data, transforms it using AWS Lambdas, and loads it for future analysis with AWS Athena.

## AWS Architecture

![Architectural Diagram](https://github.com/yangrchen/spotify-etl-aws/blob/main/aws-arch.png)

## AWS Services Used

1. **Amazon S3**
2. **AWS CloudWatch Events**
3. **AWS Lambda**
4. **AWS Glue Data Catalog**
5. **AWS Athena**

## Project Workflow

1. A cron schedule is defined in CloudWatch that acts as a trigger for the Lambda function that extracts the Spotify data (runs daily).
2. The Lambda function writes the raw data into an S3 bucket with a **raw-data/** key.
3. A separate Lambda function is triggered which transforms the raw data and stores it into the S3 bucket with a **transformed-data/** key.
4. AWS Glue is used to crawl the data and create the tables in the data catalog for later analysis.
5. Amazon Athena queries and analyzes the newly created tables.
