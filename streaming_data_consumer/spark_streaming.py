from streaming_config_params.config import *
from streaming_data_compression.compression_handler import *

# delta lake parameters
base_path = "./event-hub"
delta_output_path = f"{base_path}/data/event-hub-capture"
os.makedirs(delta_output_path, exist_ok=True)

# general program execution parameters
mins_to_simulated_failure = 1000000000
print(f"mins_to_simulated_failure: {mins_to_simulated_failure}")


# define the Event Hub configuration
ehConf = {}
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(f"{event_hub_connection_str};EntityPath={eventhub_name}")

# read from the Event Hub
df = spark.readStream.format("eventhubs") \
    .options(**ehConf) \
    .load()

# safely decode the base64 encoded data in the 'body' column
decoded_df = df.withColumn("decoded_body", expr("decode(unbase64(body), 'UTF-8')"))

# track the start time
start_time = time.time()

# define a function to process each micro-batch
def process_batch(batch_df, batch_id):

    # structure and show a sample of the data
    schema = StructType([
        StructField("sensor_id", IntegerType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("timestamp", DoubleType(), True)
    ])
    batch_df_parsed = batch_df.withColumn("parsed", from_json(col("decoded_body"), schema))
    batch_df_final = batch_df_parsed.select(col("parsed.sensor_id"), col("parsed.temperature"), col("parsed.humidity"), col("parsed.timestamp"))
    
    # apply compression to real time data stream using zlib and base64 
    batch_df_final = batch_df_final.withColumn("decoded_body_compressed", compress_code_udf(batch_df.decoded_body))
    print(f"batch_df_final: {batch_df_final.count()}")


    # write event hub data dataFrame to delta lake
    batch_df_final.write.format("delta").mode("append").save(delta_output_path)
    # #spark.sql(f"OPTIMIZE '{delta_output_path}'")
    # spark.sql(f"VACUUM '{delta_output_path}'")


    # calculate elapsed time in minutes
    elapsed_time_minutes = (time.time() - start_time) / 60
    print(f"elapsed_time_minutes: {elapsed_time_minutes}")


    # check if elapsed time is greater than 'mins_to_simulated_failure'
    if elapsed_time_minutes > mins_to_simulated_failure:
        raise StreamingQueryException(f"simulated error after {mins_to_simulated_failure} minutes...")


# define the checkpoint location
checkpointLocation = f"{base_path}/checkpoint"

# process the decoded stream and write it with checkpointing using foreachBatch
query = decoded_df.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", checkpointLocation) \
    .start()

# query.awaitTermination()