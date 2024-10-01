
# library imports
import base64, glob, hashlib, os, json, random, time
from datetime import datetime
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, lit, from_json, udf
from pyspark.sql.utils import StreamingQueryException
from pyspark.sql.types import BinaryType, StructType, StructField, IntegerType, DoubleType, StringType
# install requirements.txt file (e.g. azure-eventhub)
from streaming_config_params.install_requirements import *

# connection parameters
eventhub_name = "my-event-hub-5" # MODIFY
event_hub_connection_str = "Endpoint=sb://alt-event-hub.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=XXXXXXXXX" # MODIFY
event_hub_consumer_group = "$Default" # MODIFY


def is_running_in_databricks():
    """
    check if code is running in Databricks or locally in IDE
    """
    # Databricks typically sets these environment variables
    if 'DATABRICKS_RUNTIME_VERSION' in os.environ:
        print("code is running in databricks....\n")
        return True
    else:
        print("code is running locally....\n")
        return False


# initialize Spark session outside the conditional
if is_running_in_databricks():
    spark = SparkSession.getActiveSession()
    # Initialize dbutils
    from pyspark.dbutils import DBUtils 
    dbutils = DBUtils(spark)
else: # local IDE run
    spark = SparkSession.builder.appName("EventHubConsumer") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.21") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    
# Ensure Spark session is initialized
if not spark:
    raise Exception("Spark session could not be created.")
# set spark context (sc)
spark_context = spark.sparkContext


try: # install requirements.txt file (e.g. azure-eventhub) and restart Python
    install_requirements(requirements_file = f"{os.path.dirname(os.path.abspath(__file__))}/requirements.txt")
except: 
    install_requirements(requirements_file = f"requirements.txt")
if is_running_in_databricks(): 
    dbutils.library.restartPython() 


# additional imports after we install the requirements.txt file
import msgpack
import zstandard as zstd
from azure.eventhub import EventData, EventHubProducerClient, EventHubConsumerClient
from delta import * # has configure_spark_with_delta_pip() Python function