from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, unbase64, base64, split
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

stedi_events_schema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", StringType()),
        StructField("riskDate", StringType())
    ]
)

spark = SparkSession.builder.appName("sparkpyeventskafkastreamtoconsole").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

stedi_events_raw_df = spark\
    .readStream\
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "stedi-events") \
    .option("startingOffsets", "earliest") \
    .load()

stedi_events_raw_df = stedi_events_raw_df.selectExpr("cast(key as string) key", "cast(value as string) value")
stedi_events_raw_df\
    .withColumn("value", from_json("value", stedi_events_schema))\
    .select(col("value.*"))\
    .createOrReplaceTempView("CustomerRisk")

customerRiskStreamingDF = spark.sql("select customer, score from CustomerRisk")
customerRiskStreamingDF.writeStream.format("console").outputMode("append").start().awaitTermination()

# It should output like this:
#
# +--------------------+-----
# |customer           |score|
# +--------------------+-----+
# |Spencer.Davis@tes...| 8.0|
# +--------------------+-----
# Run the python script by running the command from the terminal:
# /home/workspace/submit-event-kafka-streaming.sh
# Verify the data looks correct 