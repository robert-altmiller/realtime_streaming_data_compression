from streaming_data_compression.decompression_handler import *


# Define paths
eh_base_path = "/event-hub"
data_base_path = f"{eh_base_path}/data"
data_output_path = f"./{data_base_path}/event-hub-capture"

# decompression handler class
dh_class = DecompressionHandler(None)
dh_class.set_gz_folder_path(f"{data_output_path}/*.gz")
df = dh_class.decompress_gz_into_spark_dataframe()
df.show(5, truncate = False)