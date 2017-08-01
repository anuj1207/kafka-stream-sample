package kafka.streams

import java.util.Properties

import org.apache.kafka.common.serialization.{Serde, Serdes, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.StreamsConfig._

object Constants{

  def getStreamConf = {
    val streamsConfiguration = new Properties()
    streamsConfiguration.put(APPLICATION_ID_CONFIG, "Streaming-QuickStart")
    streamsConfiguration.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    streamsConfiguration.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    streamsConfiguration.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    streamsConfiguration
  }

  val stringSer = new StringSerializer
  val stringDe = new StringDeserializer
  val stringSerde: Serde[String] = Serdes.String()
  val firstInTopic = "topic-1"
  val secondInTopic = "topic-2"
  val firstOutTopic = "topic-out-1"
  val secondOutTopic = "topic-out-2"

}