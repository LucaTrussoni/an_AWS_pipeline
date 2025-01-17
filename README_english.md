# An example of ETL pipeline with AWS

## The Problem

We need to develop an ETL pipeline for a customer who wants to perform analysis on cryptocurrency files, powered by AWS. The required actions are:
* uploading files to S3
* data transformation
* upload data to redshift
* check data availability for further analysis (SQL queries/BI software)
For each currency to be loaded, two files are provided: Storico.csv and Trend.csv (with different and non-standardized names for each currency). The Storico.csv file contains the Data and Price columns to be loaded, reported daily with -1 as a missing value. The Trend.csv file contains the weekly Google trend. The request places emphasis on the parallelism and scalability of the pipeline execution, which must be independent for each currency. The files are provided by the user that uploads them to S3.

## The Solution
### Files

| File    | Description |
| -------- | ------- |
| load_raw_files.py  | uploads data to S3 (see below) |
| user.key | example of authentication data |
| BTC_EUR_Historical_Data.csv | example of historical data |
| XMR_EUR Kraken Historica Data.csv | example of historical data |
| google_trend_bitcoin.csv | example of historical data |
| google_trend_monero.csv | example of historical data |
| ClearBuckets.py | ETL GLue script (see below) |
| bronze2silver.py | ETL GLue script (see below) |
| silver2gold.py | ETL GLue script (see below) |
| RedshiftUpload.py | ETL GLue script (see below) |

### Scripts

A solution based on a state machine implemented with step functions that invokes ETL glue components is proposed.

### Uploading data

The script **load_raw_files.py** uploads to S3 the files that are present in the directory in which it is launched. Authentication information are stored in the separate secret.key file.

### Step functions

The state machine is defined in the file **cryptomachine.json**

![figura1](https://github.com/LucaTrussoni/an_AWS_pipeline/blob/b4e545dfa049ead17204d670be8e21085aebc050/cryptomachine.png)

For each **cryptocurrency**, three steps contained in different scripts are performed:
* the **bronze2silver.py** script takes the data from the raw bucket to the silver bucket, managing missing values. The script receives the parameters HIST_FILE (filename containing the historical prices), TREND_FILE (filename containing the Google trend), TICKER (three-character string containing the currency ticker), DROP (Y or N depending on whether we want to delete or fill a missing value in the historical quotes file with the last valid price). The script creates in the silver bucket the paths QQQ_hist and QQQ_trend, where QQQ is the currency ticker, containing the silver files in parquet format.
* the **silver2gold.py** script takes the data from the silver bucket to the gold bucket, aligning historical prices and Google trend data and adding a moving average of prices and a linear interpolation of Google trend. The script receives the TICKER parameter (the currency ticker). The script creates in the gold bucket with the path QQQ, i.e. the currency ticker, with a parquet file containing the appropriately processed data
* the **RedshiftUpload.py** script uploads the data to redshift (in the crypto database). The script receives the TICKER parameter (currency ticker). The script uploads the data to the crypto database in a redshift workgroup, leveraging the connection crypto-connection. The database must contain the market_data schema in which is created a table with the same name as the ticker. If the table is present it is replaced with the new one.

Before execution, the **ClearBuckets.py** script takes care of clearing the contents of the silver and gold buckets.

### Execution
The scripts are executed in the MyETLGlue role which contains the AmazonDMSRedshiftS3Role, AmazonRedshiftAllCommandsFullAccess, AmazonS3FullAccess and AWSGlueServiceRole policies (although it would be better to adopt more restrictive policies). In AWS, parallel state machine nodes are configured to crash as soon as it fails
any of the parallel processes, stopping the flow of the other pipelines which could instead proceed undisturbed. To overcome this behavior, a failure of the pipeline of each currency is managed by a "Pass" that intercepts the broken execution flow: if one pipeline fails, the others proceed without the parallel process failing as a whole. The figure shows an example of the execution flow in which an error was artificially caused in the pipeline that manages Monero:

![figura2](https://github.com/LucaTrussoni/an_AWS_pipeline/blob/effa42126f6c66c991e2997ef1ed5976ed420a1b/error_mngmt.png)

As expected, the execution does not end when the parallel process fails, blocking the bitcoin pipeline, but proceeds thanks to the activation of the pass node.
Once the pipeline has been executed, this is how the data appears in redshift:

![figura3](https://github.com/LucaTrussoni/an_AWS_pipeline/blob/effa42126f6c66c991e2997ef1ed5976ed420a1b/Redshift-caricamento-btc.png)
