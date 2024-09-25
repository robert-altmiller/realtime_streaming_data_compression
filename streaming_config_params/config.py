# library imports
import base64, gzip, hashlib, os, json, random, time, zlib
from datetime import datetime
import pandas as pd
from azure.eventhub import EventData, EventHubProducerClient, EventHubConsumerClient
from pyspark import SparkContext
from pyspark.sql import SparkSession
from delta import * # has configure_spark_with_delta_pip()
from pyspark.sql.functions import col, expr, lit, from_json, udf
from pyspark.sql.utils import StreamingQueryException
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from streaming_config_params.install_requirements import *


# connection parameters
eventhub_name = "my-event-hub-2" # MODIFY
event_hub_connection_str = "Endpoint=sb://alt-event-hub.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=****************" # MODIFY
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

# create spark session and install requirements (e.g. azure-eventhub)
if is_running_in_databricks() == False:
    # initialize new local IDE Spark session

    spark = SparkSession.getActiveSession()
    if spark: # check is spark is already running
        print("Spark session is already running.")
    else:
        print("No active Spark session found.")
        builder_delta = SparkSession.builder.appName("EventHubConsumer") \
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.21") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.adaptive.enabled", "false")  # Disable AQE
        # initialize SparkSession and SparkContext
        spark = builder_delta.getOrCreate()
        spark_context= spark.sparkContext
# install requirements.txt file (e.g. azure-eventhub)
install_requirements(requirements_file = f'{os.path.dirname(os.path.abspath(__file__))}/requirements.txt')