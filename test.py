# # Use findspark to import pyspark as a python module
# import findspark
# findspark.init('C:/spark')
# import sys
# from pyspark.sql import SparkSession
# from pyspark.sql import types
# from pyspark.sql import functions as F
# from pyspark.sql.functions import udf
# from config import mysql_database_name, mysql_table_name, mysql_hostname, mysql_port
# from config import get_cot, get_vix, get_stock_volume, bid_levels, ask_levels

# import os
# os.environ["PYSPARK_PYTHON"] = sys.executable
# os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


# from config import event_list_repl, event_values, mysql_user, mysql_password, pubsub_topics
# import logging


# from google.cloud import pubsub_v1
# import json



x={
  "Timestamp": "2026-05-01 14:30:00",
  "Nonfarm_Payrolls": 175000,
  "Unemployment_Rate": 4.1,
  "Core_CPI": 0.3,
}

x={
  "Timestamp": "2026-05-01 14:30:00",
  "VIX": 18.43
}

print(x)

# print("✅✅✅✅✅✅✅✅✅✅ inports done")


# mysql_driver = 'com.mysql.jdbc.Driver'
# mysql_jdbc_url = 'jdbc:mysql://' + mysql_hostname + ':' + mysql_port + '/' + mysql_database_name

# print("✅✅✅✅✅✅✅✅✅✅ line 21")


# spark = SparkSession.builder \
#     .master("local[1]") \
#     .appName("Stock_data_streaming") \
#     .config("spark.driver.host", "localhost") \
#     .config("spark.driver.bindAddress", "127.0.0.1") \
#     .config("spark.executor.instances", "1") \
#     .config("spark.python.worker.reuse", "false") \
#     .config("spark.jars", "jar_files/mysql-connector-java-5.1.48.jar") \
#     .config("spark.driver.extraClassPath", "jar_files/mysql-connector-java-5.1.48.jar") \
# #     .getOrCreate()


# # print("URL:", mysql_jdbc_url)
# # print("USER:", mysql_user)


# import google.auth
# credentials, project = google.auth.default()
# print("AUTH ACCOUNT:", credentials.service_account_email if hasattr(credentials, 'service_account_email') else "user account")
# print("PROJECT:", project)

