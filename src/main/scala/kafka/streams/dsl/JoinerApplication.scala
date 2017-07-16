package kafka.streams.dsl

import java.util.Properties

import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.kstream.{JoinWindows, KStream, KStreamBuilder}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

object JoinerApplication extends App {


  val streamsConfiguration = new Properties()
  streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "Streaming-QuickStart")
  streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
  streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)

  val stringSerde: Serde[String] = Serdes.String()
  val firstInTopic = "topic-1"
  val secondInTopic = "topic-2"
  val outTopic = "topic-out"
  val builder = new KStreamBuilder

  /**
    * registering source topic for streaming
    * */
  val stream1: KStream[String, String] = builder.stream(firstInTopic)
  val stream2: KStream[String, String] = builder.stream(secondInTopic)

  /**
    * Data receiving from topic-1
    * sending to another topic with upper case characters
    * */
  stream1.mapValues(_.toUpperCase).to(outTopic)


  /**
    * Joining streams subscribed to two different topics
    * the data will be joined according to the anonymous valueJoiner method
    * */
  val joinedStream: KStream[String, String] = stream1.join(
    stream2,
    (value1: String, value2: String) => value1 + value2,
    JoinWindows.of(1000),
    stringSerde,
    stringSerde,
    stringSerde
  )

  /**now this joined stream is sending joined data to outTopic*/
  joinedStream.to(outTopic)

  /**creating object of kafkaStreams and starting streaming*/
  val stream: KafkaStreams = new KafkaStreams(builder, streamsConfiguration)
  stream.start()
}
