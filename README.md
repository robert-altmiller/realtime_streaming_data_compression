# Real Time Streaming Data Compression For Large JSON Payloads

## Fast and Efficient Compression and Decompression for Large JSON Payloads From An Azure Event Hub<br><br>

__Every industry today deals with real-time streaming data.  Real-time streaming data can come from a variety of sources, each suited to different applications and needs.__

- IoT Devices: Internet of Things (IoT) devices are a major source of real-time data. Sensors on these devices can stream data about temperature, humidity, and motion and are useful in industries like agriculture, manufacturing, and smart homes.
- Social Media Platforms: Services like Twitter and Facebook generate vast amounts of real-time data through user interactions such as posts, tweets, likes, and comments. This data is used for keyword and sentiment analysis, trending topics, and social media monitoring.
- Financial Markets: Stock exchanges, cryptocurrency markets, and forex exchanges provide real-time data about market conditions, trading volumes, and price changes. This data is crucial for algorithmic trading, market analysis, and financial forecasting.
- Vehicle and Traffic Sensors: Real-time data from vehicle telematics and traffic sensors is used in navigation systems, urban planning, and real-time traffic management.
- Healthcare Monitoring Devices: In healthcare, real-time data streaming from medical devices, such as heart rate monitors and glucose monitors help to support remote patient monitoring and emergency alerts.

## What are some challenges with streaming data?

__Streaming data presents many opportunities for insights and better decision making.  However, there are also challenges that organizations must manage effectively.__

- Streaming data often arrives at high velocity and in large volumes. Managing this rapid influx of data without losing or overlooking important information requires robust infrastructure and scalable processing systems.
- The quality of streaming data can be inconsistent and have missing values, duplicates, or errors due to sensor malfunctions or network issues.  Advanced filtering, validation, and error-handling techniques are needed to handle this.
- Integrating streaming data with batch processing systems and other legacy data infrastructures can be challenging. This requires careful architectural considerations.
- Different applications have varying requirements and service level agreements for how quickly data needs to be processed and made available.
- Storing streaming data for historical analysis, trend detection, or compliance purposes requires solutions that balance data accessibility and long-term storage needs.
- Ensuring data security and privacy in real-time systems requires robust encryption and access controls.
- Efficiently managing compute, storage, and network resources in an environment where data load can spike unpredictably is crucial.  Advanced resource scheduling algorithms can help with this.
- Systems must be able to scale up and down dynamically based on the volume of data. This scalability needs to be seamless to avoid performance bottlenecks and system outages.

## Why is important to compress real time streaming data?

Compressing real-time streaming data is crucial for several reasons:

- Compression reduces the amount of data that needs to be transmitted over networks. This is especially important for streaming data, which can consume significant bandwidth. By compressing data, you can reduce network congestion.
- Data transmission and storage costs can be substantial, especially with large-scale data streaming. Compression helps reduce these costs by minimizing the amount of data that needs to be stored or transmitted.
- By compressing streaming data, companies can store more data within the same physical storage space.
- Compressed data can be processed more quickly and enables complex analytics in real-time which leads to better insights and decision making.
- Compression helps mitigate the effects of increased data volume and makes it easier to scale an application without degrading performance.

## What does the JSON payload look like before and after compression?

We use 'zlib' and 'base64' Python libraries to encode the real-time streaming JSON payload and store it in a JSON arrary as '__compressed_decoded_body__'.  Here is what the real-time streaming JSON payload looks like after we apply a compression Python user defined function (UDF) to it:

  ![compressed_payload.png](/readme_images/compressed_payload.png)

We then take all of the different stored compressed JSON files and store them as individual *.gz" files.  A *.gz file is a file that has been compressed using the Gzip (GNU zip) compression algorithm, and Gzip is effective at reducing the size of text files like source code, HTML, or log files.  These *.gz files also integrate with Spark dataframes seamlessly.  In some of our own testing the file storage size is reduce by around 40% - 50% when you store large real-time data JSON payloads as *.gz files.<br>

Next we read all the *.gz files into a Spark dataframe, and then decompress the '__compressed_decoded_body__' column into a '__decompressed_decoded_body__' column using a decompression Python UDF.  This allows us to store the JSON payloads the cost effectively in the cloud or on-premise, and access the data immediately using Spark for analytics, downstream transformations, and insights.  Reading the compressed data into a Spark dataframe provides faster data reads due to smaller amounts of data being transferred over the network or disk I/O. This also reduce the time it takes to load data into memory before any transformations or actions are applied.

Here is what the real-time streaming JSON payload looks like after we read the compressed *.gz file into a Spark dataframe and then apply a decompression UDF to it:

  ![decompressed_payload.png](/readme_images/decompressed_payload.png)

## How do you get started?

- Step 1: Clone Down the Github Repository: __https://github.com/robert-altmiller/realtime_streaming_data_compression__

  ![clone_down_gh_repository.png](/readme_images/clone_down_gh_repository.png)

- Step 2: Update Azure Event Hub Connection Parameters in the '__streaming_config_params__' folder --> '__config.py__' Python file.  The '__event_hub_connection_str__' parameter in the screenshot below is the 'shared access policy' for the Azure Event Hubs namespace.  This Endpoint connection string has access to all event hubs in the Azure Event Hubs Namespace.  The second screenshot below shows where you can get the 'shared access policy' Endpoint connection string in the Azure Event Hubs Namespace resource.  

  You will also need to update '__eventhub_name__' and '__event_hub_consumer_group__' parameters to match your configuration.

  ![update_az_eh_conn_params.png](/readme_images/update_az_eh_conn_params.png)

  ![az_eh_sas_conn_string.png](/readme_images/az_eh_sas_conn_string.png)

- Step 3: Update the JSON payload used with EventHubSender class (optional).  You can find it in the '__streaming_data_producer__' folder --> '__event_hub_producer.py__' Python file.  If you have your own custom JSON payload you can overwrite this '__event_data__' dictionary below with your custom JSON payload and dynamically insert synthetic data into it to simulate different event payloads.

  ![update_json_payload.png](/readme_images/update_json_payload.png)

- Step 4: Run the code locally in your integrated development (IDE) environment or in Databricks. In the '__unit_tests__' folder you will find the following Python files: 

  - [1-test_event_hub_producer.py](/unit_tests/1-test_event_hub_producer.py)
  - [2-test_event_hub_consumer.py](/unit_tests/2-test_event_hub_consumer.py)
  - [3-test_join_gzips_and_decompress.py](unit_tests/3-test_join_gzips_and_decompress.py)
  
  Run them in chronological order.  The '__1-test_event_hub_producer.py__' will stream events to an Azure Event Hub, the '__2-test_event_hub_consumer.py__' will read events from the event hub, compress the payload and also create an MD5 has unique key of the payload, store these two fields in a local JSON file, and compress the JSON file into a *.gz local file using Spark Structured Streaming, Pandas, Zlib, Base64, Hashlib, and Gzip Python libraries.  The '__3-test_join_gzips_and_decompress.py__' will read all the *.gz compressed local files with compressed payload data into a Spark Dataframe, and then decompress the payload data for use in downstream analytics, transformations, workflows, and streaming applications.

  ![steps_to_run_demo.png](/readme_images/steps_to_run_demo.png)

  __IMPORTANT__: If you are running the three Python files above in Databricks please install the following Maven package '__com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.21__' on your Databricks single node cluster prior to executing the three Python files.

  ![maven_package_on_dbricks_cluster.png](/readme_images/maven_package_on_dbricks_cluster.png)

- Step 5: Understand where the JSON and *.gz compressed files with compressed event hub payload data and Spark structured streaming checkpoint data are stored and read from in a local IDE or Databricks environment.

  If you are running 'Step 4' in a '__local IDE__' you can find the '__checkpoint__' and '__compressed payload files__' under the '__unit_tests__' folder --> '__event-hub__' folder in the '__checkpoint__' and '__data__' folders (see image below).  It is _important_ to remember that this '__event-hub__' folder is only created in the local IDE after running the '__2-test_event_hub_consumer.py__ Python file in the '__unit_tests__' folder which reads and processes Azure Event Hub events using Spark Structured Streaming.

  ![eh_folder_with_compressed_data_local_ide.png](/readme_images/eh_folder_with_compressed_data_local_ide.png)

  If you are running 'Step 4' in a '__Databricks__' workspace you can find the '__compressed payload files__' under the '__unit_tests__' folder --> '__event-hub__' folder in the '__data__' folder (see image below).  It is _important_ to remember that this '__event-hub__' folder is only created in the Databricks after running the '__2-test_event_hub_consumer.py__ Python file in the '__unit_tests__' folder which reads and processes and stores Azure Event Hub events locally using Spark Structured Streaming.  

  ![eh_folder_with_compressed_data_databricks.png](/readme_images/eh_folder_with_compressed_data_databricks.png)

  __IMPORTANT__: The Spark Structured Streaming '__checkpoint__' folder is NOT stored in the '__event-hub__' folder in Databricks.  It is created and stored in the Databricks File System (DBFS) in the following location: '__dbfs:/Workspace/Users/[YOUR EMAIL]/realtime_streaming_data_compression/unit_tests/event-hub__' (see below).

  ![checkpoint_folder_in_databricks.png](/readme_images/checkpoint_folder_in_databricks.png)

## How do I ignore stale events in the Azure Event Hub and only process the newest events only?

If you have a requirement to ignore old events or clear a backlog queue of events in the Azure Event Hub, you can accomplish this by deleting the '__checkpoint__' folder in your local IDE or in Databricks. Once you delete this '__checkpoint__' folder, the event processor will reset its state and start processing only the latest events in the stream, rather than resuming from the previously stored checkpoint.