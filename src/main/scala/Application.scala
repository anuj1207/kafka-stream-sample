import java.util.Properties

import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.kstream.{JoinWindows, KStream, KStreamBuilder}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

/**
  * Created by anuj on 7/9/17.
  */
object Application extends App {


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

  val stream1: KStream[String, String] = builder.stream(firstInTopic)
  val stream2: KStream[String, String] = builder.stream(secondInTopic)

  val joinedStream: KStream[String, String] = stream1.join(
    stream2,
    (value1: String, value2: String) => value1 + value2,
    JoinWindows.of(1000),
    stringSerde,
    stringSerde,
    stringSerde
  )
  joinedStream.to(outTopic)
//  stream1.mapValues(_.toUpperCase).to(outTopic)
  val stream: KafkaStreams = new KafkaStreams(builder, streamsConfiguration)
  stream.start()
}
