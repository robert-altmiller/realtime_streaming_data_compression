# library imports
from streaming_data_consumer.event_hub_consumer import *

eh_data_check_point_path = f"{os.path.dirname(os.path.dirname(os.path.abspath(__file__)))}/unit_tests/event-hub"
print(f"eh_data_check_point_path: {eh_data_check_point_path}")

event_hub_consumer = EventHubConsumer(
    spark = spark,
    spark_context = spark_context,
    eh_base_path = eh_data_check_point_path,
    event_hub_connection_str = event_hub_connection_str, 
    eventhub_name = eventhub_name,
    mins_to_simulated_failure = 1000000000
)
event_hub_consumer.start_streaming()