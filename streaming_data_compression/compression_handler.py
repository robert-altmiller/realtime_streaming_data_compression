# library imports
from streaming_config_params.config import *


class CompressionHandler:
    
    def __init__(self, original_file_path=None, output_file_path=None):
        self.original_file_path = original_file_path
        self.output_file_path = output_file_path


    def set_original_file_path(self, filepath):
        """
        set the original_file_path variable
        """
        self.original_file_path = filepath

        
    def set_output_file_path(self, filepath):
        """
        set the output_file_path variable
        """
        self.output_file_path = filepath


    def md5_hash_udf(self):
        """
        return a spark UDF that compresses data into an md5 hash using hashlib.
        """
        def md5_hash_data(data):
            return hashlib.md5(data.encode()).hexdigest()
        return udf(md5_hash_data, StringType())


    def compress_data_udf(self):
        """
        return a spark UDF that compresses data using compression Python libraries.
        """
        def compress_data(data):
            compressed_data = msgpack.packb(data.encode('utf-8'))
            return compressed_data
        return udf(compress_data, BinaryType())


    def decompress_data_udf(self):
        """
        return a spark UDF that decompresses compressed data.
        """
        def decompress_data(compressed_data):
            decompressed_data = msgpack.unpackb(compressed_data).decode('utf-8')
            return decompressed_data
        return udf(decompress_data, StringType())


    def compress_file_with_parquet(self, compression_method = "zstd"):
        """
        Reads a JSON file and stores it as a Parquet file using Spark with specified compression.
        """
        # Read the original file
        df = spark.read.json(self.original_file_path)
        # Store the DataFrame as a Parquet file with specified compression
        df.write.mode('overwrite').option("compression", compression_method).parquet(self.output_file_path)


    def compress_file_with_gz(self, compresslevel=9):
        """
        compresses a file using gz and saves it to compressed_file_path.
        """
        # Read the original file
        with open(self.original_file_path, 'r') as f:
            json_data = f.read()
        # Compress the JSON file with gz
        with gzip.open(self.output_file_path, 'wt', encoding='utf-8', compresslevel=compresslevel) as gz_file:
            gz_file.write(json_data)
        print(f"File compressed to {self.output_file_path} using gz.")


    def get_file_sizes(self):
        """
        returns the size of the original JSON and compressed gz files in KB.
        """
        original_size = os.path.getsize(self.original_file_path)
        output_size = os.path.getsize(self.output_file_path)
        return original_size / 1024, output_size / 1024


    def print_compression_savings(self):
        """
        prints the size of the original file, compressed file, and the compression ratio.
        """
        original_size_kb, output_size_kb = self.get_file_sizes()
        # Calculate original_size and output_size compression ratio
        compression_ratio = 100 * (original_size_kb - output_size_kb) / original_size_kb
        print(f"\n{self.original_file_path} original size: {original_size_kb:.2f} KB")
        print(f"{self.output_file_path} compressed size: {output_size_kb:.2f} KB")
        print(f"Compression Savings Ratio: {compression_ratio:.2f}%\n")