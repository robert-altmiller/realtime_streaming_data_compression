
# library import
import os, time, msgpack, shutil, subprocess, math, glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta import configure_spark_with_delta_pip

def is_running_in_databricks():
    """
    check if code is running in Databricks or locally in IDE
    """
    # Databricks typically sets these environment variables
    if 'DATABRICKS_RUNTIME_VERSION' in os.environ:
        return True
    else:
        return False


if is_running_in_databricks() == False:
    """initialize Spark session"""
    builder = SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()


def decompress_data_udf():
    """return a spark UDF that decompresses compressed msgpack data."""
    def decompress_data(compressed_data):
        decompressed_data = msgpack.unpackb(compressed_data).decode('utf-8')
        return decompressed_data
    return udf(decompress_data, StringType())

        
def time_df_read_benchmark(read_method, input_path, decompress_data = False):
    """timed benchmark for writing a Spark dataframe"""
    # Start the timer for benchmarking
    start_time = time.time()

    if read_method == "parquet":
        df = spark.read.parquet(input_path)
    if decompress_data == True:
        decompress_udf = decompress_data_udf()
        #df = df.withColumn("decompressed_decoded_body", decompress_udf(df.compressed_decoded_body)).drop("compressed_decoded_body")
        df = df.withColumn("decoded_body", decompress_udf(df.decoded_body))
        
    print(f"df.count(): {df.count()}")
    df.show(10)
    # Stop the timer for benchmarking
    end_time = time.time()
    time_taken = end_time - start_time
    print(f"Time taken to read all files into DataFrame, decompress data: {decompress_data}, and show total records: {time_taken:.2f} seconds\n")


def time_df_write_benchmark(read_method, write_method, input_path, output_path, agg_by_col, decompress_data = False):
    """timed benchmark for writing a Spark dataframe"""
    
    # Start the timer for benchmarking
    start_time = time.time()
    
    if read_method == "parquet":
        df = spark.read.parquet(input_path)
    if decompress_data == True:
        decompress_udf = decompress_data_udf()
        #df = df.withColumn("decoded_body", decompress_udf(df.compressed_decoded_body)).drop("compressed_decoded_body")
        df = df.withColumn("decoded_body", decompress_udf(df.decoded_body))
    
    # Apply simple aggregation
    df_agg = df.groupBy(agg_by_col).count()
    df_agg.printSchema()
    df_agg.show(10, truncate = False)

    # Write in the dataframe
    if write_method == "parquet":
        df_agg.write.format("parquet").mode("overwrite").save(output_path)
    elif write_method == "delta":
        df_agg.write.format("delta").mode("overwrite").save(output_path)
    
    # Stop the timer for benchmarking
    end_time = time.time()
    time_taken = end_time - start_time
    # Print write benchmarking results
    print(f"\nTime taken to read all files into DataFrame, decompress data: {decompress_data}, and write simple aggregation: {time_taken:.2f} seconds")


def get_folder_size(folder_path):
    # Run the `du -sk` command to get folder size in kilobytes
    result = subprocess.run(['du', '-sk', folder_path], stdout=subprocess.PIPE)
    # Extract the size from the output (it's in kilobytes)
    folder_size_kb = int(result.stdout.split()[0].decode('utf-8'))
    # Convert from kilobytes to megabytes
    folder_size_mb = folder_size_kb / 1024
    return folder_size_mb


      
print("\nRun Benchmark Test -- Parquet Z Standard 'Single Round' Compression:\n")
if is_running_in_databricks():
    path = "/dbfs/Workspace/Users/robert.altmiller@databricks.com/realtime_streaming_data_compression/unit_tests/event-hub/data/event-hub-capture"
    inputpath = f"{path}/*_original.parquet"
    outputpath = f"{path}/original_parquets2/benchmark_write"
    dbutils.fs.rm(outputpath, True)
else:
    path = "/Users/robert.altmiller/repos/projects/github/realtime_streaming_data_compression/unit_tests/event-hub/data/event-hub-capture"
    inputpath = f"{path}/*_original.parquet"
    outputpath = f"{path}/original_parquets/benchmark_write"
    if os.path.exists(outputpath) and os.path.isdir(outputpath):
        shutil.rmtree(outputpath)
print(f"Total Size MB of All Read Data: {get_folder_size(path)}")
time_df_read_benchmark(read_method = "parquet", input_path = inputpath)
time_df_write_benchmark(read_method = "parquet", write_method = "parquet", input_path = inputpath, output_path = outputpath, agg_by_col = "decoded_body")


print("\nRun Benchmark Test -- MSGPACK Data Payload Compression + Parquet Z Standard Single Round Compression:\n")
if is_running_in_databricks():
    path = "/dbfs/Workspace/Users/robert.altmiller@databricks.com/realtime_streaming_data_compression/unit_tests/event-hub/data/event-hub-capture"
    inputpath = f"{path}/*_compressed.parquet"
    outputpath = f"{path}/compressed_parquets/benchmark_write"
    dbutils.fs.rm(outputpath, True)
else: 
    path = "/Users/robert.altmiller/repos/projects/github/realtime_streaming_data_compression/unit_tests/event-hub/data/event-hub-capture"
    inputpath = f"{path}/*_compressed.parquet"
    outputpath = f"{path}/compressed_parquets/benchmark_write"
if os.path.exists(outputpath) and os.path.isdir(outputpath):
    shutil.rmtree(outputpath)
print(f"Total Size MB of All Read Data: {get_folder_size(path)}")
time_df_read_benchmark(read_method = "parquet", input_path = inputpath, decompress_data = True)
time_df_write_benchmark(read_method = "parquet", write_method = "parquet", input_path = inputpath, output_path = outputpath, agg_by_col = "decoded_body", decompress_data = True)
