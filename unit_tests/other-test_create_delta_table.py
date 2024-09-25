from pyspark.sql import SparkSession
import os
from delta import configure_spark_with_delta_pip


builder = SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0")

# Create Spark session using Delta configuration
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Example DataFrame and Delta append operation
data = spark.range(0, 5)

# Define the output path for the Delta table
output_path = f"{os.path.dirname(os.path.abspath(__file__))}/event-hub/data/event-hub-capture"

# Append data to the Delta table
data.write.format("delta").mode("append").save(output_path)

# Confirming successful append
print(f"Data appended to {output_path}")