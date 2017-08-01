# High Level DSL

This package contains example of two methods of Kafka Streams high level DSL

1. mapValues:
This method maps throughs every value being recieved through the topic being subscribed
and any modification can be done on that value. This example changes every character into upper case characters

2. join:
This method applies inner join on two streams and returns a new streams which will have the combined data(according to the logic provided in paramenters using value joiner class) of both the streams.
And this data can be sent to one single topic.

Note: join method joins data of two streams according to the timestamp of data. Here in this example we are using the ingestion time as timestamp which is embedded automatically when data is being ingested.
If you want to work on eventt time then extract timestamp from the value using a separate timestamp extractor class which have to be provided in properties of KafkaStreams.

Syntax: 'streamsConfiguration.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, classOf[TimestampExtractor])'

# To run this example

1. Start zookeeper
2. Start kafka broker
3. Run GeneratorApp : which generates data and sends it to two different topics and keep sending it using an akka scheduler
4. Run JoinerApplication : this application contains the operation of high level dsl
5. After that you can create a separate consumer on terminal using following command to verify the data

   bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic topic-out-1 --from- beginning

   Topic: 'topic-out-1' will contain data with character converted to upper case
   Topic: 'topic-out-2' will contain data combined from two streams 