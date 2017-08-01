package kafka.streams.processorAPI

import kafka.streams.Constants._
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.KStreamBuilder

object Application extends App{
  val builder = new KStreamBuilder
  val streamProcessor = new StreamProcessor

  //calling method which is using the implementation of custom processor
  streamProcessor.orderStreamData(builder, firstInTopic, firstOutTopic)

  //starting streaming
  val stream: KafkaStreams = new KafkaStreams(builder, getStreamConf)
  stream.start()
}
