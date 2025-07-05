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



@dlt.view(
    name="trans_flights"
)
def trans_flights():
    df = spark.readStream.format("delta")\
        .load('/Volumes/workspace/bronze/bronzevolume/flights/data/')

    df = df.withColumn("flight_date", to_date(col("flight_Date")))\
        .withColumn("modified_date", current_timestamp())\
        .drop("_rescued_data")
    return df

dlt.create_streaming_table("silver_flights")

dlt.create_auto_cdc_flow(
    target = "silver_flights",
    source = "trans_flights",
    keys = ["flight_id"],
    sequence_by = col("modified_date"),
    stored_as_scd_type = 1
)



#for Cusomter data tranformation to Silver Layer

@dlt.view(
    name="trans_passengers"
)
def trans_passengers():
    df = spark.readStream.format("delta")\
        .load('/Volumes/workspace/bronze/bronzevolume/customers/data/')
    
    df = df.withColumn("modified_date", current_timestamp())\
        .drop("_rescued_data")
    return df

dlt.create_streaming_table("silver_passengers")

dlt.create_auto_cdc_flow(
    target = "silver_passengers",
    source = "trans_passengers",
    keys = ["passenger_id"],
    sequence_by = col("modified_date"),
    stored_as_scd_type = 1
)


#for Airports data tranformation to Silver Layer

@dlt.view(
    name="trans_airports"
)
def trans_airports():
    df = spark.readStream.format("delta")\
        .load('/Volumes/workspace/bronze/bronzevolume/airports/data/')
    
    df = df.withColumn("modified_date", current_timestamp())\
        .drop("_rescued_data")
    return df

dlt.create_streaming_table("silver_airports")

dlt.create_auto_cdc_flow(
    target = "silver_airports",
    source = "trans_airports",
    keys = ["airport_id"],
    sequence_by = col("modified_date"),
    stored_as_scd_type = 1
)


#Silver Bussiness View

@dlt.table(
    name = "silver_bussiness_view"
)
def silver_business():
    df = dlt.readStream("silver_bookings")\
        .join(dlt.readStream("silver_flights"), ["flight_id"])\
        .join(dlt.readStream("silver_passengers"), ["passenger_id"])\
        .join(dlt.readStream("silver_airports"), ["airport_id"])
    return df

