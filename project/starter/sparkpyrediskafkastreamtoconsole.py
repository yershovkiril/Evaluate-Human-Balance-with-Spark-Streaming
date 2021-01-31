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

spark = SparkSession.builder.appName("sparkpyrediskafkastreamtoconsole").getOrCreate()
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
emailAndBirthYearStreamingDF.writeStream.format("console").outputMode("append").start().awaitTermination()

# The output should look like this:
# +--------------------+-----               
# | email         |birthYear|
# +--------------------+-----
# |Gail.Spencer@test...|1963|
# |Craig.Lincoln@tes...|1962|
# |  Edward.Wu@test.com|1961|
# |Santosh.Phillips@...|1960|
# |Sarah.Lincoln@tes...|1959|
# |Sean.Howard@test.com|1958|
# |Sarah.Clark@test.com|1957|
# +--------------------+-----

# Run the python script by running the command from the terminal:
# /home/workspace/submit-redis-kafka-streaming.sh
# Verify the data looks correct 