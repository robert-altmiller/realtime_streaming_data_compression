# library imports
from streaming_data_consumer.event_hub_consumer import *


if is_running_in_databricks():
    # get the current notebook path using dbutils
    notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    eh_base_path = f"/Workspace{os.path.dirname(notebook_path)}/event-hub"
else:
    eh_base_path = f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/unit_tests/event-hub"
print(f"eh_base_path: {eh_base_path}")


# EventHubConsumer usage example
event_hub_consumer = EventHubConsumer(
    spark = spark,
    spark_context = spark_context,
    eh_base_path = eh_base_path,
    event_hub_connection_str = event_hub_connection_str, 
    eventhub_name = eventhub_name,
    mins_to_simulated_failure = 1000000000
)
event_hub_consumer.start_streaming()