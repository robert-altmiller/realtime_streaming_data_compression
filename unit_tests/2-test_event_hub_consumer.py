from streaming_data_compression.compression_handler import *

# Define paths
eh_base_path = "/event-hub"
data_base_path = f"{eh_base_path}/data"
data_output_path = f"./{data_base_path}/event-hub-capture"

# Create the Delta output path if it doesn't exist
if not os.path.exists(data_output_path):
    os.makedirs(data_output_path)

# General program execution parameters
mins_to_simulated_failure = 1000000000
print(f"mins_to_simulated_failure: {mins_to_simulated_failure}")


# Define the Event Hub configuration directly as a Python dictionary
ehConf = {}
ehConf['eventhubs.connectionString'] = spark_context._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(f"{event_hub_connection_str};EntityPath={eventhub_name}")

# Read from the Event Hub using the defined configuration
df = spark.readStream \
    .format("eventhubs") \
    .options(**ehConf) \
    .load()

decoded_df = df.withColumn("decoded_body", expr("decode(unbase64(body), 'UTF-8')"))


def process_batch(batch_df, batch_id):
    import base64
    
    # compression handler class
    ch_class = CompressionHandler(None, None)
    # function to compress data
    compress_data_udf = ch_class.compress_data_udf()
    # function to create data md5 hash
    md5_hash_data_udf = ch_class.md5_hash_udf()

    try:
        print(f"Processing batch: {batch_id}")        
        # batch_df_parsed = batch_df.withColumn("parsed", from_json(col("decoded_body"), schema))
        batch_df_parsed = batch_df.select("decoded_body") # JSON payload only
        batch_df_parsed.show(5)
        
        # write the parsed data into compressed JSON files to save on storage costs
        if batch_df_parsed.count() > 0:
            batch_df_parsed = batch_df_parsed \
                .withColumn("compressed_decoded_body", compress_data_udf(batch_df_parsed.decoded_body)) \
                .withColumn("md5_hash_decoded_body", md5_hash_data_udf(batch_df_parsed.decoded_body)) \
                .select("compressed_decoded_body", "md5_hash_decoded_body")
            batch_df_parsed_pandas = batch_df_parsed.toPandas()
            # Generate a unique filename using a timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file_name = f'output_file_{timestamp}.json'
            output_file_path = f"{os.path.dirname(os.path.abspath(__file__))}{data_output_path.replace(".","")}/{output_file_name}"
            # save the Pandas DataFrame to a new JSON file with the unique filename
            batch_df_parsed_pandas.to_json(output_file_path, orient='records', lines=True)
            # compress JSON file further using gz
            ch_class.set_original_file_path(output_file_path)
            ch_class.set_gz_file_path(output_file_path.replace(".json", ".gz"))
            ch_class.compress_file_with_gz()
            ch_class.print_gz_compression_savings()
            #batch_df_parsed.write.format("delta").mode("append").save(output_path)
    
    except Exception as e:
        print(f"Error processing batch {batch_id}: {str(e)}")
        raise e  # Re-raise the error to be caught by the Spark query


query = decoded_df.writeStream \
    .format("json") \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "./event-hub/checkpoint") \
    .start()
query.awaitTermination()










# batch_df_parsed_compressed = batch_df_parsed \
#     .withColumn("payload_compressed", compress_code_udf(lit(batch_df_parsed.decoded_body)))

# Write the decoded data to the console for verification
# query = batch_df_parsed.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

# query.awaitTermination()