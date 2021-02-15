### Data Warehouse Project

With this project we are moving user and song databases of Sparkify to AWS cloud servises. The original data is located ASW S3 services. With the ETL process we build, we will move this data to staging tables first and then using this staging tables to final tables in AWS Redhift.

### More info about data

There are 2 buckets of data in AWS S3 services. They are described as below.

* Song data: s3://udacity-dend/song_data
* Log data: s3://udacity-dend/log_data

Using these two AWS S3 bucket we will move them to the staging table with COPY function. And then we will extract, transform and load to the star schema we have designed as shown below.

![](images/diagram.png)

staging_song (staging table)
staging_events (staging table)

Using these two staging tables we created a star schema consist of 4 dimention table and one fact table as listed below:

* time (dimension table)
* song (dimension table)
* user (dimension table)
* artist (dimension table)

* songplay (fact table)


### Data Warehouse Setup

We created a IAM user in AWS account. Using the secret key and access key we created S3 and Redshift clients. Finaly we created a redshift cluster and compteleted the dwh.cfg file with the necessary credentials


### Execution of the Project

We first run the create_tables on terminal. It will use the sql statements we created in sql_queries.py file. Then finally we run the etl.py file to fill the tables we created with the source data.

