from streaming_data_compression.decompression_handler import *


if is_running_in_databricks():
    # get the current notebook path using dbutils
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    eh_base_path = f"/Workspace{os.path.dirname(notebook_path)}/event-hub"
else:
    eh_base_path = f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/unit_tests/event-hub"
print(f"eh_base_path: {eh_base_path}")

data_basepath = f"{eh_base_path}/data"
data_output_path = f"{data_basepath}/event-hub-capture/"


# DecompressionHandler usage example
method = "parquet" # or "gzip"
dh_class = DecompressionHandler(None)
if is_running_in_databricks():
    dh_class.set_data_folder_path(f"file:{data_output_path}/*_compressed.{method}")
else: 
    dh_class.set_data_folder_path(f"{data_output_path}/*.{method}")
df = dh_class.decompress_data_into_spark_dataframe(method = method, compression_method = "zstd")
# df.show(5, truncate = False)