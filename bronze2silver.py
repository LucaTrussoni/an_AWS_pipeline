import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','HIST_FILE','TREND_FILE','TICKER','DROP'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

# =============================================================

from pyspark.sql.functions import to_date,when,last,regexp_replace
from pyspark.sql.types import StructType,StructField,StringType,DateType,IntegerType
from pyspark.sql.window import Window

def process_hist_file(file_name,bronze_bucket_path,silver_bucket_path,ticker,drop=False):
    """
        Usage: process_hist_file(file_name,
                                bronze_bucket_path,
                                silver_bucket_path,
                                ticker,
                                drop=False)
        The function process_hist_file gets the historical file from bronze to
        silver bucket, taking care of missing value. The function will save init
        the silver bucket in a parquet called XXX_hist where XXX is the ticker
        
        Input:
        file_name: the csv file to process, string
        bronze_bucket_path: source bucket, string
        silver_bucket_path: target bucket, string
        ticker: the ticker of the currency, string (3 char typically)
        drop: if true missing values are dropped, if false they are
        filled in a carry over last price logic
        
        Output: none
    """
    # Creating the schema to read the input df and reading it
    schema = StructType([
        StructField("Date", StringType(), True),
        StructField("Price", StringType(), True),
        StructField("Open", StringType(), True),
        StructField("High", StringType(), True),
        StructField("Low", StringType(), True),
        StructField("Vol", StringType(), True),
        StructField("Change", StringType(), True)])
    dataframe = spark.read.format("csv")\
        .option("header", "true")\
        .schema(schema)\
        .load(bronze_bucket_path+'BTC_EUR_Historical_Data.csv')\
    # Taking care of formats (spark 3.3, we do not have to_number)
    dataframe = dataframe.withColumn("Date", to_date(dataframe["Date"], "MM/dd/yyyy"))
    dataframe = dataframe.withColumn("Price", regexp_replace(dataframe.Price, ',', ''))
    dataframe = dataframe.withColumn("Price", dataframe.Price.cast("float"))
    # Taking care of missing values
    if drop==True:
        silver_dataframe = dataframe.select("Date","Price").where(dataframe.Price!=-1)
    else:
        window=Window.orderBy("date").rowsBetween(-10,-1) # looking back for 10 working days...
        dataframe=dataframe.withColumn("Price",
            when(dataframe["Price"]!=-1,dataframe["Price"]) # if Price is not -1 take Price, otherwise...
            .otherwise(last(when(dataframe["Price"]!=-1,dataframe["Price"]),True).over(window)))
            # look for the last non -1 value.
        silver_dataframe = dataframe.select("Date","Price")
    # Now silve_dataframe needs to be written
    silver_dataframe.write.parquet(silver_bucket_path+ticker+"_hist")

def process_trend_file(file_name,bronze_bucket_path,silver_bucket_path,ticker):
    """
        Usage: process_trend_file(file_name,
                                bronze_bucket_path,
                                silver_bucket_path,
                                ticker)
        This function does not do much, just takes the input csv file
        and rewrites it in the silver bucket in parquet format with
        an appropriate file
        
        Input:
        file_name: the csv file to process, string
        bronze_bucket_path: source bucket, string
        silver_bucket_path: target bucket, string
        ticker: the ticker of the currency, string (3 char typically)
        
        Output:none
    """
    schema = StructType([
        StructField("date", DateType(), True),
        StructField("g_trend", IntegerType(), True)
    ])
    dataframe = spark.read.format("csv").option("header", "true").schema(schema).load(bronze_bucket_path+file_name)
    dataframe.write.parquet(silver_bucket_path+ticker+"_trend")

# ====================================
# THE CODE OF THE SCRIPT STARTS HERE!
# ====================================
logger = glueContext.get_logger()
logger.info("This is bronze2silver script.")
logger.info(f"Starting execution for ticker {args['TICKER']}")
bronze_bucket_path="s3://YOURPREFIX.progettoaws.raw/"
silver_bucket_path="s3://YOURPREFIX.progettoaws.silver/"
logger.info("Processing historical file")
drop_missing=False
if args['DROP']=='Y':
    logger.info('Missing values will be dropped')
    drop_missing=True
else:
    logger.info('Carry over last price logic for missing values')
process_hist_file(args['HIST_FILE'], bronze_bucket_path, silver_bucket_path, args['TICKER'],drop_missing)
logger.info('Processing google trend file')
process_trend_file(args['TREND_FILE'], bronze_bucket_path, silver_bucket_path, args['TICKER'])
logger.info('Processing completed')