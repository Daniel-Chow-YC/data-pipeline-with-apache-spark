# ETL Pipeline with Apache Spark

## Task
Build an ETL pipeline for a data lake hosted on S3. To complete this project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3 as a set of dimensional tables. You'll deploy this Spark process on a cluster using AWS.

## To run the project
- Update the configuration file dl.cfg with the required AWS credentials.
- Create an S3 bucket named **udacity-dend-dcyc-output** where the output results will be stored. (You can use a different name for this bucket but you will need to change the value of the output_data variable in etl.py to match this)
- Then run ``python etl.py``

## Project structure
- dl.cfg: File with AWS credentials.
- etl.py: Program that loads songs and events data from S3, processes the data into dimensional tables using Spark,and then loads them back into S3 as parquet files
- README.md: Current file, contains information about the project.
