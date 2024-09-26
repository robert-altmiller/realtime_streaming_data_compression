# library imports
from streaming_config_params.config import *


class CompressionHandler:
    
    def __init__(self, original_file_path=None, gz_file_path=None):
        self.original_file_path = original_file_path
        self.gz_file_path = gz_file_path


    def set_original_file_path(self, filepath):
        """
        set the original_file_path variable
        """
        self.original_file_path = filepath

        
    def set_gz_file_path(self, filepath):
        """
        set the gz_file_path variable
        """
        self.gz_file_path = filepath


    def md5_hash_udf(self):
        """
        return a spark UDF that compresses data into an md5 hash using hashlib.
        """
        def md5_hash_data(data):
            return hashlib.md5(data.encode()).hexdigest()
        return udf(md5_hash_data, StringType())


    def compress_data_udf(self):
        """
        return a spark UDF that compresses data using zlib-compressed and base64 Python libraries .
        """
        def compress_data(data):
            compressed_data = zlib.compress(data.encode('utf-8'))
            return base64.b64encode(compressed_data).decode('utf-8')
        return udf(compress_data, StringType())


    def decompress_data_udf(self):
        """
        return a spark UDF that decompresses zlib-compressed data that is base64 encoded.
        """
        def decompress_data(compressed_data):
            decompressed_data = zlib.decompress(base64.b64decode(compressed_data.encode('utf-8')))
            return decompressed_data.decode('utf-8')
        return udf(decompress_data, StringType())


    def compress_file_with_gz(self, compresslevel=9):
        """
        compresses a file using gz and saves it to compressed_file_path.
        """
        # Read the original file
        with open(self.original_file_path, 'r') as f:
            json_data = f.read()
        # Compress the JSON file with gz
        with gzip.open(self.gz_file_path, 'wt', encoding='utf-8', compresslevel=compresslevel) as gz_file:
            gz_file.write(json_data)
        print(f"File compressed to {self.gz_file_path} using gz.")


    def get_gz_json_file_sizes(self):
        """
        returns the size of the original JSON and compressed gz files in KB.
        """
        original_size = os.path.getsize(self.original_file_path)
        gz_size = os.path.getsize(self.gz_file_path)
        return original_size / 1024, gz_size / 1024


    def print_gz_compression_savings(self):
        """
        prints the size of the original json file, compressed gz file, and the compression ratio.
        """
        original_size_kb, gz_size_kb = self.get_gz_json_file_sizes()
        # Calculate JSON to GZ compression ratio
        compression_ratio = 100 * (original_size_kb - gz_size_kb) / original_size_kb
        print(f"\n{self.original_file_path} JSON original size: {original_size_kb:.2f} KB")
        print(f"{self.gz_file_path} GZ compressed size: {gz_size_kb:.2f} KB")
        print(f"JSON to GZ Compression Savings: {compression_ratio:.2f}%\n")