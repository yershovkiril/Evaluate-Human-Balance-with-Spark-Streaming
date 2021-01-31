from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType


redis_server_schema = StructType(
    [
        StructField("key", StringType()),
        StructField("existType", StringType()),
        StructField("ch", StringType()),
        StructField("incr", BooleanType()),
        StructField("zSetEntries", ArrayType(
            StructType([
                StructField("element", StringType()),
                StructField("score", StringType())
            ]))
        )
    ]
)

customer_schema = StructType(
    [
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", StringType())
    ]
)

stedi_events_schema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", StringType()),
        StructField("riskDate", StringType())
    ]
)

spark = SparkSession.builder.appName("sparkpykafkajoin").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

redis_server_raw_df = spark\
    .readStream\
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "redis-server") \
    .option("startingOffsets", "earliest") \
    .load()

redis_server_raw_df = redis_server_raw_df.selectExpr("cast(key as string) key", "cast(value as string) value")
redis_server_raw_df\
    .withColumn("value", from_json("value", redis_server_schema))\
    .select(col("value.*"))\
    .createOrReplaceTempView("RedisSortedSet")

customers_df = spark.sql("select key, zSetEntries[0].element as encodedCustomer from RedisSortedSet")
customers_df = customers_df.withColumn("encodedCustomer", unbase64(customers_df.encodedCustomer).cast("string"))

customers_df\
    .withColumn("encodedCustomer", from_json("encodedCustomer", customer_schema))\
    .select(col("encodedCustomer.*"))\
    .createOrReplaceTempView("CustomerRecords")

emailAndBirthDayStreamingDF = spark.sql("select email, birthDay from CustomerRecords "
                                        "where email is not null and birthDay is not null")
emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF\
    .select("email", split(col("birthDay"), "-").getItem(0).alias("birthYear"))

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
riskScoreByBirthYear = emailAndBirthYearStreamingDF.join(customerRiskStreamingDF, expr("email = customer"))
riskScoreByBirthYear\
    .selectExpr("cast(email as string) key", "to_json(struct(*)) value")\
    .writeStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "kafka:19092")\
    .option("topic", "customer-risk")\
    .option("checkpointLocation", "/tmp/kafkacheckpoint2")\
    .start()\
    .awaitTermination()
