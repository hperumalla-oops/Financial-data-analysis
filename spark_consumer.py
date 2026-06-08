# Use findspark to import pyspark as a python module
import findspark
findspark.init('C:/spark')
import sys
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from config import mysql_database_name, mysql_table_name, mysql_hostname, mysql_port
from config import get_cot, get_vix, get_stock_volume, bid_levels, ask_levels

import os
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


from config import event_list_repl, event_values, mysql_user, mysql_password, pubsub_topics
import logging


from google.cloud import pubsub_v1
import json

def pull_messages(subscription_path):
    subscriber = pubsub_v1.SubscriberClient()
    response = subscriber.pull(
        request={"subscription": subscription_path, "max_messages": 100}
    )
    messages = []
    ack_ids = []
    for msg in response.received_messages:
        messages.append(json.loads(msg.message.data.decode("utf-8")))
        ack_ids.append(msg.ack_id)
    if ack_ids:
        subscriber.acknowledge(
            request={"subscription": subscription_path, "ack_ids": ack_ids}
        )
    return messages



print("✅✅✅✅✅✅✅✅✅✅ inports done")


mysql_driver = 'com.mysql.jdbc.Driver'
# mysql_jdbc_url = 'jdbc:mysql://' + mysql_hostname + ':' + mysql_port + '/' + mysql_database_name

mysql_jdbc_url = "jdbc:mysql://127.0.0.1:3306/finance_database?useSSL=false&allowPublicKeyRetrieval=true"

print("✅✅✅✅✅✅✅✅✅✅ line 51")

import tempfile
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("Stock_data_streaming") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.local.dir", "C:/tmp/spark") \
    .config("spark.python.worker.reuse", "false") \
    .config("spark.jars", "jar_files/mysql-connector-java-8.0.28.jar") \
    .config("spark.driver.extraClassPath", "jar_files/mysql-connector-java-8.0.28.jar") \
    .config("spark.network.timeout", "800s") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .getOrCreate()


print("✅✅✅✅✅✅✅✅✅✅ line 67")

# Set number of output partitions (low values speed up processing)
spark.conf.set("spark.sql.shuffle.partitions", 5)



def write_stream_to_mysql(dataFrame, id):
    """Writes each batch of streaming dataFrame to MySQL/MariaDB

    """
    db_properties = {"user": mysql_user,
        "password": mysql_password,
        "driver": mysql_driver}

    if dataFrame.rdd.isEmpty():
        pass
    else:
        dataFrame \
          .write \
          .jdbc(url=mysql_jdbc_url,
            table=mysql_table_name,
            mode='append',
            properties=db_properties)

# Define VIX schema
# {"VIX": 16.04, "Timestamp": "2020-02-07 09:26:12"}
schema_vix = types.StructType([
    types.StructField('VIX', types.FloatType()),
    types.StructField('Timestamp', types.StringType())
])


sub_vix = pubsub_topics['vix'].replace('topics', 'subscriptions') + '-sub'
data_vix = pull_messages(sub_vix)

print("✅✅✅✅✅✅✅✅✅✅ line 106")

print("data_vix:", data_vix)
print("type:", type(data_vix[0]) if data_vix else "empty")

# sys.exit()


df_vix = spark.createDataFrame(data_vix) \
    .select(F.col("VIX"), F.col("Timestamp")) \
    .withColumn("Timestamp_vix", F.to_timestamp(F.col("Timestamp"), "yyyy-MM-dd HH:mm:ss")) \
    .drop("Timestamp")


# Round timestamps down to nearest 5 minutes
df_vix = df_vix \
  .withColumn("Timestamp_vix_floor", (F.floor(F.unix_timestamp("Timestamp_vix") / (5 * 60)) * 5 * 60).cast("timestamp"))

# Apply watermark
df_vix = df_vix.withWatermark("Timestamp_vix", "5 minutes")

schema_volume = types.StructType([
    types.StructField('1_open', types.FloatType()),
    types.StructField('2_high', types.FloatType()),
    types.StructField('3_low', types.FloatType()),
    types.StructField('4_close', types.FloatType()),
    types.StructField('5_volume', types.IntegerType()),
    types.StructField('Timestamp', types.StringType())
    ])

sub_ind= pubsub_topics['intraday'].replace('topics', 'subscriptions') + '-sub'
data_ind = pull_messages(sub_ind)


df_ind = spark.createDataFrame(data_ind) \
    .withColumn("Timestamp_ind", F.to_timestamp(F.col("Timestamp"), "yyyy-MM-dd HH:mm:ss")) \
    .drop("Timestamp")

# Round timestamps down to nearest 5 minutes
df_ind = df_ind \
  .withColumn("Timestamp_ind_floor", (F.floor(F.unix_timestamp("Timestamp_ind") / (5 * 60)) * 5 * 60).cast("timestamp"))
  

# Apply watermark
df_ind = df_ind.withWatermark("Timestamp_ind", "5 minutes")
schema_deep = types.StructType([types.StructField('Timestamp', types.StringType())])
 
print(df_ind.columns)
print("✅✅✅✅✅✅✅✅✅✅ line 154")

sub_volume = pubsub_topics['volume'].replace('topics', 'subscriptions') + '-sub'
data_volume = pull_messages(sub_volume)
data_volume = [x for x in data_volume if x is not None]

print("data_volume count:", len(data_volume))
print("data_volume sample:", data_volume[:2])



df_volume = spark.createDataFrame(data_volume) \
    .select(F.col("Volume").cast("long").alias("VOLUME"), F.col("Timestamp")) \
    .withColumn("Timestamp_volume", F.to_timestamp(F.col("Timestamp"), "yyyy-MM-dd HH:mm:ss")) \
    .drop("Timestamp")


# Round timestamps down to nearest 5 minutes
df_volume = df_volume \
  .withColumn("Timestamp_vol_floor", (F.floor(F.unix_timestamp("Timestamp_volume") / (5 * 60)) * 5 * 60).cast("timestamp"))
  # .withColumn("Timestamp_vol_floor", (F.floor(F.unix_timestamp("Timestamp_vol") / (5 * 60)) * 5 * 60).cast("timestamp"))

# Apply watermark
df_volume = df_volume.withWatermark("Timestamp_vol", "5 minutes")



schema_cot = types.StructType([types.StructField('Timestamp', types.StringType())])

# Fields in case of currencies and stocks: ['Asset', 'Leveraged']
# In the event of metals, grains, softs: [Managed']
for field in ['Asset', 'Leveraged']:
    schema_cot.add(types.StructField(field, types.StructType([
        types.StructField('{}_long_pos'.format(field), types.IntegerType()),
        types.StructField('{}_long_pos_change'.format(field), types.FloatType()),
        types.StructField('{}_long_open_int'.format(field), types.FloatType()),
        types.StructField('{}_short_pos'.format(field), types.IntegerType()),
        types.StructField('{}_short_pos_change'.format(field), types.FloatType()),
        types.StructField('{}_short_open_int'.format(field), types.FloatType())
        ])))



sub_cot= pubsub_topics['cot'].replace('topics', 'subscriptions') + '-sub'
data_cot = pull_messages(sub_cot)


df_cot = spark.createDataFrame(data_cot) \
    .withColumn("Timestamp_cot", F.to_timestamp(F.col("Timestamp"), "yyyy-MM-dd HH:mm:ss")) \
    .drop("Timestamp")

# Round timestamps down to nearest 5 minutes
df_cot = df_cot \
  .withColumn("Timestamp_cot_floor", (F.floor(F.unix_timestamp("Timestamp_cot") / (5 * 60)) * 5 * 60).cast("timestamp"))

# Apply watermark
df_cot = df_cot.withWatermark("Timestamp_cot", "5 minutes")

schema_ind = types.StructType([types.StructField('Timestamp', types.StringType())])

for field in event_list_repl:
    schema_ind.add(types.StructField(field, types.StructType([
        types.StructField(ind, types.FloatType()) for ind in event_values
        ])))


df_joined = df_vix.crossJoin(df_cot).crossJoin(df_ind)


print("df_joined columns before volume join:", df_joined.columns)

# Fill missing values
df_joined = df_joined.fillna(0)
print("✅✅✅✅✅✅✅✅✅✅ line 228")

# df_joined.select("VIX", "Timestamp", "VOLUME").show(5)

print(df_joined.columns)

df_joined.show(5)


try:
    from pyspark.sql.functions import to_json
        
    spark.sparkContext.setLogLevel("ERROR")

    # MapReduce job — count messages per minute
    rdd = spark.sparkContext.parallelize(data_vix)

    # Map — extract minute
    mapped = rdd.map(lambda x: (x['Timestamp'][:16], 1))

    # Reduce — count per minute
    reduced = mapped.reduceByKey(lambda a, b: a + b)

    print("Messages per minute:", reduced.collect())

    # Convert all map columns to JSON strings
    map_cols = [f.name for f in df_joined.schema.fields if str(f.dataType).startswith("Map")]
    for col_name in map_cols:
        df_joined = df_joined.withColumn(col_name, to_json(F.col(col_name)))

    mysql_cols = [f.name for f in spark.read.jdbc(
    url=mysql_jdbc_url,
    table=mysql_table_name,
    properties={"user": mysql_user, "password": mysql_password, "driver": "com.mysql.cj.jdbc.Driver"}
    ).schema.fields]
    mysql_cols.remove("ID")  # auto increment



    df_to_write = df_joined.select(*[c for c in df_joined.columns if c in mysql_cols])

    mysql_cols_set = set(mysql_cols)
    df_cols_set = set(df_joined.columns)
    print("✅✅✅✅✅✅✅✅✅✅ line 257")  

    print("in MySQL not in df:", mysql_cols_set - df_cols_set)
    print("in df not in MySQL:", df_cols_set - mysql_cols_set)

    common_cols = list(mysql_cols_set & df_cols_set)
    print("common cols:", common_cols)

    if common_cols:
        df_to_write = df_joined.select(*common_cols)
        write_stream_to_mysql(df_to_write, 0)
    else:
        print("NO COMMON COLUMNS")
        
    print("WRITE SUCCESS")
except Exception as e:
    print("WRITE FAILED:", e)


df_joined.printSchema()
print("✅✅✅✅✅✅✅✅✅✅ spark_consumer successfull")
