# library imports
from streaming_config_params.config import *


class EventHubSender:
    
    def __init__(self, eventhub_name, event_hub_connection_str, consumer_group="$Default", show_events = True,
                 num_events_per_batch=1000, target_events_per_minute=100000, total_events_to_send=100000000):
        # Event hub parameters
        self.eventhub_name = eventhub_name
        self.event_hub_connection_str = event_hub_connection_str
        self.consumer_group = consumer_group

        # Producer client
        self.producer_client = EventHubProducerClient.from_connection_string(event_hub_connection_str, eventhub_name=eventhub_name)
        
        # Configuration constants
        self.num_events_per_batch = num_events_per_batch
        self.target_events_per_minute = target_events_per_minute
        self.total_events_to_send = total_events_to_send
        self.seconds_per_minute = 60
        self.sleep_time = self.seconds_per_minute / (self.target_events_per_minute / self.num_events_per_batch)
        self.show_events = show_events

        # Event tracking
        self.start_time = time.time()
        self.events_sent = 0


    def generate_event_data(self, i):
        """
        Generate simulated event data.
        """
        event_data = {
            "sensor_id": i,
            "temperature": random.uniform(20, 30),
            "humidity": random.uniform(40, 60),
            "timestamp": time.time()
        }
        return event_data


    def encode_event_data(self, event_data):
        """
        Convert event data to JSON and Base64 encode it.
        """
        event_data_str = json.dumps(event_data)
        encoded_event_data = base64.b64encode(event_data_str.encode()).decode()
        return EventData(encoded_event_data)


    def show_events_data(self, event_data):
        """
        Print event data.
        """
        print(event_data)


    def send_event_batch(self):
        """
        Creates and sends a batch of events to EventHub.
        """
        try:
            while self.events_sent < self.total_events_to_send:  # Limit for number of events
                elapsed_time = time.time() - self.start_time
                if elapsed_time >= self.seconds_per_minute:
                    self.start_time = time.time()
                    self.events_sent = 0

                # Create a new batch
                event_data_batch = self.producer_client.create_batch()

                for i in range(self.num_events_per_batch):
                    event_data = self.generate_event_data(i)
                    event = self.encode_event_data(event_data)
                    if self.show_events:
                        self.show_events_data(event_data)
                    try:
                        event_data_batch.add(event)
                    except ValueError:
                        # Batch is full, send it and create a new one
                        self.producer_client.send_batch(event_data_batch)
                        event_data_batch = self.producer_client.create_batch()
                        event_data_batch.add(event)

                # Send any remaining events in the final batch
                if len(event_data_batch) > 0:
                    self.producer_client.send_batch(event_data_batch)

                self.events_sent += self.num_events_per_batch
                time.sleep(self.sleep_time)

        except Exception as e:
            print("Exception:", e)
        finally:
            self.producer_client.close()


    def run(self):
        """
        Entry point to start sending events.
        """
        self.send_event_batch()