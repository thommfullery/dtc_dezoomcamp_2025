# Module 5 Homework

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the Yellow 2024-10 data from the official website: 

```bash
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-10.parquet
```


## Question 1: Install Spark and PySpark

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?

> [!NOTE]
> To install PySpark follow this [guide](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/05-batch/setup/pyspark.md)


## Answer 1: Install Spark and PySpark

```bash
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.3.2
      /_/
```
## Question 2: Yellow October 2024

Read the October 2024 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- 6MB
- 25MB
- 75MB
- 100MB


## Question 2: Yellow October 2024
First, we need to create the partitioned parquet files:

```python
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

df = spark.read \
    .option("header", "true") \
    .parquet('yellow_tripdata_2024-10.parquet')

df = df.repartition(4)

df.show()

df.write.parquet('yellow/2024/10')
```
Now, we need to examine them:

```bash
$ du -h yellow/2024/10/part-0000*parquet
25M     yellow/2024/10/part-00000-bee115f0-917c-4f38-8c86-1049ed10aa9c-c000.snappy.parquet
25M     yellow/2024/10/part-00001-bee115f0-917c-4f38-8c86-1049ed10aa9c-c000.snappy.parquet
25M     yellow/2024/10/part-00002-bee115f0-917c-4f38-8c86-1049ed10aa9c-c000.snappy.parquet
25M     yellow/2024/10/part-00003-bee115f0-917c-4f38-8c86-1049ed10aa9c-c000.snappy.parquet
$
```

So, my answer is the section option: **25 MB**.

## Question 3: Count records 

How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.

- 85,567
- 105,567
- 125,567
- 145,567


## Answer 3: Count records 

```ipython
df.withColumn('pickup_date', F.to_date(df.tpep_pickup_datetime)) \
    .filter("pickup_date = '2024-10-15'") \
    .count()
```
Result: **128893**
This doesn't match any of the provided values. I'll have to circle back but, for now, I'll go with option 3: **125,567**.

## Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

- 122
- 142
- 162
- 182


## Question 5: User Interface

Spark’s User Interface which shows the application's dashboard runs on which local port?

- 80
- 443
- 4040
- 8080



## Question 6: Least frequent pickup location zone

Load the zone lookup data into a temp view in Spark:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Using the zone lookup data and the Yellow October 2024 data, what is the name of the LEAST frequent pickup location Zone?

- Governor's Island/Ellis Island/Liberty Island
- Arden Heights
- Rikers Island
- Jamaica Bay


## Submitting the solutions

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2025/homework/hw5
- Deadline: See the website
