import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME','TICKER'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

from pyspark.sql.functions import last,first,avg,when
from pyspark.sql.window import Window

# To join datasets, we will first collect all the dates 
# Setup of names
silver_bucket_path=''
gold_bucket_path=''
source_hist=args['TICKER']+"_hist"
source_trend=args['TICKER']+"_trend"

silver_bucket_path="s3://YOURPREFIX.progettoaws.silver/"
gold_bucket_path="s3://YOURPREFIX.progettoaws.gold/"

hist_dframe=spark.read.parquet(silver_bucket_path+source_hist)
trend_dframe=spark.read.parquet(silver_bucket_path+source_trend)
all_dates = hist_dframe.select("Date").union(trend_dframe.select("Date")).sort("Date").distinct()
joined_dframe = all_dates.join(hist_dframe, on="Date", how="left_outer").join(trend_dframe, on="Date", how="left_outer")
joined_dframe.show(20)
# We will now take care of some mismatch
# First: Price column needs to be integrated with carry over last price logic
# in case some google trend date is not in the historical time series
# Second: google trend values need to be filled. Since any value refers to the
#`preceeding week, any value needs to be integrated with the following non 
# null value
window_spec_clear_null_price = Window.orderBy("Date").rowsBetween(-2,-1)
joined_dframe = joined_dframe.withColumn("Price", 
    when(joined_dframe.Price.isNotNull(),joined_dframe["Price"]).otherwise(
        last("Price", ignorenulls=True).over(window_spec_clear_null_price)))
window_spec_clear_null_trend = Window.orderBy("Date").rowsBetween(1,6)
joined_dframe = joined_dframe.withColumn("g_trend_cont",
    when(joined_dframe.g_trend.isNotNull(),joined_dframe["g_trend"]).otherwise(
        first("g_trend", ignorenulls=True).over(window_spec_clear_null_trend)))
# We will now create the new moving agerage columns
window_spec_ma_price = Window.orderBy("Date").rowsBetween(-13,0)
window_spec_ma_trend = Window.orderBy("Date").rowsBetween(-6,0)
joined_dframe = joined_dframe.withColumn("PriceMA", avg("Price").over(window_spec_ma_price))
joined_dframe = joined_dframe.withColumn("TrendMA", avg("g_trend_cont").over(window_spec_ma_trend))
# Let's change a column name for uniformity
joined_dframe=joined_dframe.drop("g_trend_cont")
# Time to write the file!
joined_dframe.write.parquet(gold_bucket_path+args['TICKER'])

# =====================================
job.commit()






