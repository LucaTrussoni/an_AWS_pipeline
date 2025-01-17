import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME','TICKER'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

target="s3://YOURPREFIX.progettoaws.gold/"+args['TICKER']+"/"
target_table="market_data."+args['TICKER']
target_sql="DROP TABLE IF EXISTS market_data."+args['TICKER']+"; CREATE TABLE IF NOT EXISTS market_data."+args['TICKER']+" (date DATE, price REAL, trend INTEGER, pricema DOUBLE PRECISION, trend_i DOUBLE PRECISION);"

# Script generated for node Amazon S3
AmazonS3_node1 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": [target], "recurse": True}, transformation_ctx="AmazonS3_node1")

# Script generated for node Change Schema
ChangeSchema_node2 = ApplyMapping.apply(frame=AmazonS3_node1, mappings=[("date", "date", "date", "date"), ("price", "float", "price", "float"), ("g_trend", "int", "trend", "int"), ("pricema", "double", "pricema", "double"), ("trendma", "double", "trend_i", "double")], transformation_ctx="ChangeSchema_node2")

# Script generated for node Amazon Redshift
AmazonRedshift_node3 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node2, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-999999999999-eu-central-1/temporary/", "useConnectionProperties": "true", "dbtable": target_table, "connectionName": "crypto-connection", "preactions": target_sql}, transformation_ctx="AmazonRedshift_node3")

job.commit()