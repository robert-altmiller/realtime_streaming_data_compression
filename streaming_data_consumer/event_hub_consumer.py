from streaming_data_compression.compression_handler import *


class EventHubConsumer:
    
    def __init__(self, spark, spark_context, event_hub_connection_str, eventhub_name, eh_base_path, mins_to_simulated_failure=1000000000):
        self.spark = spark
        self.spark_context = spark_context
        self.event_hub_connection_str = event_hub_connection_str
        self.eventhub_name = eventhub_name
        self.eh_base_path = eh_base_path
        self.data_base_path = f"{self.eh_base_path}/data"
        self.data_output_path = f"{self.data_base_path}/event-hub-capture"
        self.data_checkpoint_path = f"{self.eh_base_path}/checkpoint"
        self.mins_to_simulated_failure = mins_to_simulated_failure
        self.ehConf = {}

        # setup paths and event hub configuration
        self.setup_paths()
        self.configure_event_hub()


    def setup_paths(self):
        """
        Create the output path if it doesn't exist.
        """
        os.makedirs(self.data_output_path, exist_ok = True)


    def configure_event_hub(self):
        """Configure the Event Hub connection."""
        self.ehConf['eventhubs.connectionString'] = self.spark_context._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
            f"{self.event_hub_connection_str};EntityPath={self.eventhub_name}"
        )


    def read_stream(self):
        """
        Read the stream from the Event Hub.
        """
        return self.spark.readStream \
            .format("eventhubs") \
            .options(**self.ehConf) \
            .load()


    def decode_stream(self, df):
        """
        Decode the event hub messages.
        """
        return df.withColumn("decoded_body", expr("decode(unbase64(body), 'UTF-8')"))


    def process_batch(self, batch_df, batch_id):
        """
        Process each batch of data.
        """
        ch_class = CompressionHandler(None, None)
        compress_data_udf = ch_class.compress_data_udf()
        md5_hash_data_udf = ch_class.md5_hash_udf()

        try:
            print(f"Processing batch: {batch_id}")
            batch_df_parsed = batch_df.select("decoded_body")
            batch_df_parsed.show(5)

            # write the parsed data into compressed JSON files to save on storage costs
            if batch_df_parsed.count() > 0:
                batch_df_parsed_compressed = batch_df_parsed \
                    .withColumn("decoded_body", compress_data_udf(batch_df_parsed.decoded_body)) \
                    .select("decoded_body")
                
                # Convert to Pandas and save as a JSON file
                batch_df_parsed_pd = batch_df_parsed.select("decoded_body").toPandas()
                #batch_df_parsed_compressed_pd = batch_df_parsed_compressed.toPandas()
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_file_name = f'output_file_{timestamp}.json'
                output_file_path = os.path.join(self.data_output_path, output_file_name)
                print(f"output_file_path: {output_file_path}")
                # output the original json payloads without compression for benchmarking
                # batch_df_parsed_pd.to_json(output_file_path.replace(".json", ".json"), orient='records', lines=True)
                
                # Store original and compressed payload data as PARQUET
                if is_running_in_databricks(): output_file_path_orig = f'/dbfs{output_file_path.replace(".json", "_original.parquet")}'
                else: output_file_path_orig = output_file_path.replace(".json", "_original.parquet")
                print(f"output_file_path_orig: {output_file_path_orig}")
                batch_df_parsed.write.mode('overwrite').option("compression", "zstd").parquet(output_file_path_orig) # use 'zstandard' compression
                if is_running_in_databricks(): output_file_path_comp = f'/dbfs{output_file_path.replace(".json", "_compressed.parquet")}'
                else: output_file_path_comp = output_file_path.replace(".json", "_compressed.parquet")
                print(f"output_file_path_comp: {output_file_path_comp}")
                batch_df_parsed_compressed.write.mode('overwrite').option("compression", "zstd").parquet(output_file_path_comp)
                
                # Set the CompressionHandler class original file path (optional)
                #ch_class.set_original_file_path(output_file_path)
                
                # Store payload data as GZIP (optional)
                #ch_class.set_output_file_path(output_file_path.replace(".json", ".gz"))
                #ch_class.compress_file_with_gz()
                #ch_class.print_compression_savings()

        except Exception as e:
            print(f"Error processing batch {batch_id}: {str(e)}")
            raise e


    def start_streaming(self):
        """
        Start the streaming process.
        """
        decoded_df = self.decode_stream(self.read_stream())
        
        print(f"checkpoint_path: {self.data_checkpoint_path}")
        query = decoded_df.writeStream \
            .format("json") \
            .foreachBatch(self.process_batch) \
            .option("checkpointLocation", self.data_checkpoint_path) \
            .start()
        query.awaitTermination()