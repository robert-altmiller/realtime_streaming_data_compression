# library imports
from streaming_data_consumer.event_hub_consumer import *


event_hub_consumer = EventHubConsumer(
    spark = spark,
    spark_context = spark_context,
    eh_base_path = "./unit_tests/event-hub",
    event_hub_connection_str = event_hub_connection_str, 
    eventhub_name = eventhub_name,
    mins_to_simulated_failure = 1000000000
)
event_hub_consumer.start_streaming()