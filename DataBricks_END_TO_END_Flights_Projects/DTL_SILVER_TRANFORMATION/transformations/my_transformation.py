import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
  name="stage_bookings"
)
def stage_bookings():
    df = spark.readStream.format("delta")\
        .load('/Volumes/workspace/bronze/bronzevolume/bookings/data/')
    return df


@dlt.view(
    name="trans_bookings"
)
def trans_bookings():
    df = spark.readStream.table("stage_bookings")
    df = df.withColumn('amount', col("amount").cast("double"))\
        .withColumn("modified_date", current_timestamp())\
        .withColumn("booking_date", to_date("booking_date"))\
        .drop("_rescued_data")
    
    return df

rules = {
    "rule1":"booking_id IS NOT NULL",
    "rule2":"passenger_id IS NOT NULL"
}

@dlt.table(
    name='silver_bookings'
)
@dlt.expect_all_or_drop(rules)
def silver_bookings():
    df = spark.readStream.table("trans_bookings")
    #or we can use the below one as well
    # df =dlt.read("trans_bookings")
    return df