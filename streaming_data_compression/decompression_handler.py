# library imports
from streaming_config_params.config import *
from streaming_data_compression.compression_handler import CompressionHandler


class DecompressionHandler(CompressionHandler):
    def __init__(self, data_folder_path=None):
        # Call the parent class's constructor to initialize common attributes
        super().__init__(None, None)
        self.data_folder_path = data_folder_path


    def set_data_folder_path(self, filepath):
        """
        Set the data folder path variable.
        """
        self.data_folder_path = filepath


    def decompress_data_into_spark_dataframe(self, method = "parquet", compression_method = "gzip"):
        """
        Decompress and join all the compressed files into a Spark DataFrame.
        """
        if self.data_folder_path is None:
            raise ValueError("data_folder_path is not set. Use set_data_folder_path() to set the path.")

        # function to decompress data in the CompressionHandler class
        decompress_data_udf = self.decompress_data_udf()

        # Start the timer for benchmarking
        start_time = time.time()

        # Read all data files into a Spark dataFrame
        if method == "parquet":
            df = spark.read.parquet(self.data_folder_path)
        elif method == "gzip":
            df = spark.read.json(self.data_folder_path)
        elif method == "csv":
            all_csv_files = glob.glob(self.data_folder_path)
            df = spark.createDataFrame(pd.concat((pd.read_csv(f, compression = compression_method) for f in all_csv_files), ignore_index=True))
        else: raise ValueError(f"the method {method} is not supported")
        
        # decompress the data
        df = df.withColumn("decoded_body", decompress_data_udf(df.compressed_decoded_body)).drop("compressed_decoded_body")
        
        print(f"df.count(): {df.count()}")
        
        # Stop the timer for benchmarking
        end_time = time.time()
        time_taken = end_time - start_time
        print(f"Time taken to read all files into DataFrame, decompress data, and show total records: {time_taken:.2f} seconds\n")

        return df