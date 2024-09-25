# library imports
from streaming_config_params.config import *
from streaming_data_compression.compression_handler import CompressionHandler


class DecompressionHandler(CompressionHandler):
    def __init__(self, gz_folder_path=None):
        # Call the parent class's constructor to initialize common attributes
        super().__init__(None, None)
        self.gz_folder_path = gz_folder_path

    def set_gz_folder_path(self, filepath):
        """
        Set the gz folder path variable.
        """
        self.gz_folder_path = filepath


    def decompress_gz_into_spark_dataframe(self):
        """
        Decompress and join all the gz compressed files into a Spark DataFrame.
        """
        if self.gz_folder_path is None:
            raise ValueError("gz_folder_path is not set. Use set_gz_folder_path() to set the path.")

        # function to decompress data in the CompressionHandler class
        decompress_data_udf = self.decompress_data_udf()

        # Read all gz files into a Spark DataFrame
        df = spark.read.json(self.gz_folder_path)
        df = df.withColumn("decompressed_decoded_body", decompress_data_udf(df.compressed_decoded_body)).drop("compressed_decoded_body")
        return df