from streaming_data_producer.event_hub_producer import *


# EventHubProducer usage example
event_hub_sender = EventHubSender(
    eventhub_name = eventhub_name, 
    event_hub_connection_str = event_hub_connection_str,
    consumer_group = event_hub_consumer_group,
    num_events_per_batch = 100,
    target_events_per_minute = 100000,
    total_events_to_send = 100000000,
    show_events = True # or False
)
event_hub_sender.run()