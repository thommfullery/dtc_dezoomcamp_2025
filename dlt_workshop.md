# Question 1: dlt Version
## Install dlt
    !pip install dlt[duckdb]
## Check dlt Version
    !dlt --version
> dlt 1.6.1

# Question 2: Define & Run the Pipeline (NYC Taxi API)
## Use dlt to extract all pages of data from the API.

Steps:

1️⃣ Use the @dlt.resource decorator to define the API source.

2️⃣ Implement automatic pagination using dlt's built-in REST client.

3️⃣ Load the extracted data into DuckDB for querying.
    
    import requests
    
    BASE_API_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"
    
    import dlt
    from dlt.sources.helpers.rest_client import RESTClient
    from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
    
    
    @dlt.resource(name="ny_taxi")  # Define ny_taxi as a dlt resource
    def paginated_getter():
        client = RESTClient(
            base_url="https://us-central1-dlthub-analytics.cloudfunctions.net",
            # Define pagination strategy - page-based pagination
            paginator=PageNumberPaginator(   # <--- Pages are numbered (1, 2, 3, ...)
                base_page=1,   # <--- Start from page 1
                total_path=None    # <--- No total count of pages provided by API, pagination should stop when a page contains no result items
            )
        )
    
        for page in client.paginate("data_engineering_zoomcamp_api"):    # <--- API endpoint for retrieving taxi ride data
            yield page   # remember about memory management and yield data
    
    for page_data in paginated_getter():
        print(page_data)
    
    pipeline = dlt.pipeline(
        pipeline_name="ny_taxi_pipeline",
        destination="duckdb",
        dataset_name="ny_taxi_data"
    )
## Load the data into DuckDB to test:
    load_info = pipeline.run(paginated_getter(), table_name="rides", write_disposition="replace")
    print(load_info)
## Start a connection to your database using native duckdb connection and look what tables were generated:

    import duckdb
    from google.colab import data_table
    data_table.enable_dataframe_formatter()
    
    # A database '<pipeline_name>.duckdb' was created in working directory so just connect to it
    
    # Connect to the DuckDB database
    conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")
    
    # Set search path to the dataset
    conn.sql(f"SET search_path = '{pipeline.dataset_name}'")
    
    # Describe the dataset
    conn.sql("DESCRIBE").df()

## Results

|index|database|schema|name|column\_names|column\_types|temporary|
|---|---|---|---|---|---|---|
|0|ny\_taxi\_pipeline|ny\_taxi\_data|\_dlt\_loads|\['load\_id' 'schema\_name' 'status' 'inserted\_at' 'schema\_version\_hash'\]|\['VARCHAR' 'VARCHAR' 'BIGINT' 'TIMESTAMP WITH TIME ZONE' 'VARCHAR'\]|false|
|1|ny\_taxi\_pipeline|ny\_taxi\_data|\_dlt\_pipeline\_state|\['version' 'engine\_version' 'pipeline\_name' 'state' 'created\_at' 'version\_hash' '\_dlt\_load\_id' '\_dlt\_id'\]|\['BIGINT' 'BIGINT' 'VARCHAR' 'VARCHAR' 'TIMESTAMP WITH TIME ZONE' 'VARCHAR' 'VARCHAR' 'VARCHAR'\]|false|
|2|ny\_taxi\_pipeline|ny\_taxi\_data|\_dlt\_version|\['version' 'engine\_version' 'inserted\_at' 'schema\_name' 'version\_hash' 'schema'\]|\['BIGINT' 'BIGINT' 'TIMESTAMP WITH TIME ZONE' 'VARCHAR' 'VARCHAR' 'VARCHAR'\]|false|
|3|ny\_taxi\_pipeline|ny\_taxi\_data|rides|\['end\_lat' 'end\_lon' 'fare\_amt' 'passenger\_count' 'payment\_type' 'start\_lat' 'start\_lon' 'tip\_amt' 'tolls\_amt' 'total\_amt' 'trip\_distance' 'trip\_dropoff\_date\_time' 'trip\_pickup\_date\_time' 'surcharge' 'vendor\_name' '\_dlt\_load\_id' '\_dlt\_id' 'store\_and\_forward'\]|\['DOUBLE' 'DOUBLE' 'DOUBLE' 'BIGINT' 'VARCHAR' 'DOUBLE' 'DOUBLE' 'DOUBLE' 'DOUBLE' 'DOUBLE' 'DOUBLE' 'TIMESTAMP WITH TIME ZONE' 'TIMESTAMP WITH TIME ZONE' 'DOUBLE' 'VARCHAR' 'VARCHAR' 'VARCHAR' 'DOUBLE'\]|false|

Therefore, 4 tables are cereated.

# Question 3: Explore the loaded data
Inspect the table ride:
    df = pipeline.dataset(dataset_type="default").rides.df()
    df

## Results

|index|end\_lat|end\_lon|fare\_amt|passenger\_count|payment\_type|start\_lat|start\_lon|tip\_amt|tolls\_amt|total\_amt|trip\_distance|trip\_dropoff\_date\_time|trip\_pickup\_date\_time|surcharge|vendor\_name|\_dlt\_load\_id|\_dlt\_id|store\_and\_forward|
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|0|40\.742963|-73\.980072|45\.0|1|Credit|40\.641525|-73\.787442|9\.0|4\.15|58\.15|17\.52|2009-06-14 23:48:00+00:00|2009-06-14 23:23:00+00:00|0\.0|VTS|1739711465\.5898159|dckkmtSIpqBS8w|NaN|
|1|40\.740187|-74\.005698|6\.5|1|Credit|40\.722065|-74\.009767|1\.0|0\.0|8\.5|1\.56|2009-06-18 17:43:00+00:00|2009-06-18 17:35:00+00:00|1\.0|VTS|1739711465\.5898159|FhKhSKKZbv9xsg|NaN|
|2|40\.718043|-74\.004745|12\.5|5|Credit|40\.761945|-73\.983038|2\.0|0\.0|15\.5|3\.37|2009-06-10 18:27:00+00:00|2009-06-10 18:08:00+00:00|1\.0|VTS|1739711465\.5898159|oHSg99nWox1Jag|NaN|
|3|40\.739637|-73\.985233|4\.9|1|CASH|40\.749802|-73\.992247|0\.0|0\.0|5\.4|1\.11|2009-06-14 23:58:00+00:00|2009-06-14 23:54:00+00:00|0\.5|VTS|1739711465\.5898159|ntrLwx1CoZ2Chg|NaN|
|4|40\.730032|-73\.852693|25\.7|1|CASH|40\.776825|-73\.949233|0\.0|4\.15|29\.85|11\.09|2009-06-13 13:23:00+00:00|2009-06-13 13:01:00+00:00|0\.0|VTS|1739711465\.5898159|SQJDhvjnBt6VEQ|NaN|
|5|40\.777537|-73\.97686|7\.3|2|Credit|40\.790582|-73\.953652|2\.0|0\.0|10\.3|2\.1|2009-06-10 19:52:00+00:00|2009-06-10 19:43:00+00:00|1\.0|VTS|1739711465\.5898159|zCI36gXF0hG1hw|NaN|
|6|40\.770277|-73\.962125|3\.7|1|Credit|40\.767147|-73\.966408|1\.0|0\.0|5\.2|0\.4|2009-06-10 20:09:00+00:00|2009-06-10 20:06:00+00:00|0\.5|VTS|1739711465\.5898159|P5nLfm1ywrUGJw|NaN|
|7|40\.774043|-73\.951465|8\.1|2|CASH|40\.76175|-73\.977773|0\.0|0\.0|8\.6|2\.24|2009-06-14 21:08:00+00:00|2009-06-14 20:57:00+00:00|0\.5|VTS|1739711465\.5898159|LZcvboHU5cklQw|NaN|
|8|40\.777985|-73\.943683|6\.1|1|CASH|40\.766355|-73\.959832|0\.0|0\.0|6\.1|1\.48|2009-06-14 12:56:00+00:00|2009-06-14 12:49:00+00:00|0\.0|VTS|1739711465\.5898159|tCJcObGqge6Scg|NaN|
|9|40\.720052|-74\.009823|8\.9|1|CASH|40\.751327|-73\.987588|0\.0|0\.0|9\.9|2\.72|2009-06-10 18:13:00+00:00|2009-06-10 18:03:00+00:00|1\.0|VTS|1739711465\.5898159|nNOVLa47Ffva8Q|NaN|
|10|40\.758588|-73\.981827|4\.1|5|CASH|40\.764112|-73\.98861|0\.0|0\.0|4\.1|0\.69|2009-06-14 11:27:00+00:00|2009-06-14 11:24:00+00:00|0\.0|VTS|1739711465\.5898159|95G3qDUgCB+I2w|NaN|
|11|40\.727343|-73\.993425|2\.5|4|Credit|40\.727343|-73\.993425|4\.0|0\.0|6\.5|0\.0|2009-06-13 19:17:00+00:00|2009-06-13 19:17:00+00:00|0\.0|VTS|1739711465\.5898159|Ty/kPUk0ZqMEVA|NaN|
|...|
|9997|40\.780172|-73\.957617|6\.1|1|CASH|40\.788388|-73\.976758|0\.0|0\.0|6\.1|1\.3|2009-06-19 11:46:00+00:00|2009-06-19 11:39:00+00:00|0\.0|VTS|1739711465\.5898159|qD4r4ABLOzpQXQ|NaN|
|9998|40\.777342|-73\.957242|5\.7|1|CASH|40\.773828|-73\.95669|0\.0|0\.0|6\.2|0\.97|2009-06-17 04:19:00+00:00|2009-06-17 04:13:00+00:00|0\.5|VTS|1739711465\.5898159|Fo+b8IThyfI1Fg|NaN|
|9999|40\.757122|-73\.986293|6\.5|1|Credit|40\.756313|-73\.972948|2\.0|0\.0|8\.5|0\.92|2009-06-17 08:34:00+00:00|2009-06-17 08:24:00+00:00|0\.0|VTS|1739711465\.5898159|1p7ogN26aAsMgA|NaN|

Therefore, 10000 rows returned.

# Question 4: Trip Duration Analysis
## Run the SQL query below to calculate the average trip duration in minutes.

    with pipeline.sql_client() as client:
        res = client.execute_sql(
                """
                SELECT
                AVG(date_diff('minute', trip_pickup_date_time, trip_dropoff_date_time))
                FROM rides;
                """
            )
        # Prints column values of the first row
        print(res)

## Results
[(12.3049,)]

The average ride is 12.3049 minutes.
