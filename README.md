# Real Time Streaming Data Compression For Large JSON Payloads

## Fast and Efficient Compression and Decompression for Large JSON Payloads From An Azure Event Hub<br><br>

__Every industry today deals with real-time streaming data.  Real-time streaming data can come from a variety of sources, each suited to different applications and needs.  Here are some examples below:__

- IoT Devices: Internet of Things (IoT) devices are a major source of real-time data. Sensors on these devices can stream data about temperature, humidity, and motion and are useful in industries like agriculture, manufacturing, and smart homes.
- Social Media Platforms: Services like Twitter and Facebook generate vast amounts of real-time data through user interactions such as posts, tweets, likes, and comments. This data is used for keyword and sentiment analysis, trending topics, and social media monitoring.
- Financial Markets: Stock exchanges, cryptocurrency markets, and forex exchanges provide real-time data about market conditions, trading volumes, and price changes. This data is crucial for algorithmic trading, market analysis, and financial forecasting.
- Vehicle and Traffic Sensors: Real-time data from vehicle telematics and traffic sensors is used in navigation systems, urban planning, and real-time traffic management.
- Healthcare Monitoring Devices: In healthcare, real-time data streaming from medical devices, such as heart rate monitors and glucose monitors help to support remote patient monitoring and emergency alerts.

## What are some challenges with streaming data?

Streaming data presents many opportunities for insights and better decision making.  However, there are also challenges that organizations must manage effectively. Some of these challenges include:

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

## What does the JSON payload look like before compression and after compression

We use 'zlib' and 'base64' Python libraries to encode the real-time streaming JSON payload and store it in a JSON arrary as '__compressed_decoded_body__'.  Here is what the real-time streaming JSON payload looks like after we apply a compression Python user defined function (UDF) to it:

  ![compressed_payload.png](/readme_images/compressed_payload.png)

We then take all of the different stored compressed JSON files and store them as individual *.gz" files.  A *.gz file is a file that has been compressed using the Gzip (GNU zip) compression algorithm, and Gzip is effective at reducing the size of text files like source code, HTML, or log files.  These *.gz files also integrate with Spark dataframes seamlessly.  In some of our own testing the file storage size is reduce by around 40% - 50% when you store large real-time data JSON payloads as *.gz files.  Next we read all the *.gz files into a Spark dataframe, and then decompress the '__compressed_decoded_body__' column using a decompression Python UDF.  This allows us to store the JSON payloads the cost effectively in the cloud, and access the data immediately for analytics, downstream transformations, and insights.

Here is what the real-time streaming JSON payload looks like after we apply a decompression UDF to it:

  ![decompressed_payload.png](/readme_images/decompressed_payload.png)