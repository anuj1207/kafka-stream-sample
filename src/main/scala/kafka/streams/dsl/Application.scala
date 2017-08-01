package kafka.streams.dsl

import kafka.streams.Constants._
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.kstream.KStreamBuilder

object Application extends App{

  val streamConf = getStreamConf

  val builder = new KStreamBuilder
  val streamOperations = new StreamOperations
  val joiningWindow = 10L

  /**calling method for joining data of firstInTopic, secondInTopic*/
  val joinedStream = streamOperations.join(builder, firstInTopic, secondInTopic, joiningWindow)
  joinedStream.to(firstOutTopic)

  /**calling method that converts characters values from secondInTopic to upper case*/
  val upperCaseStream = streamOperations.toUpperCase(builder, secondInTopic)
  upperCaseStream.to(secondOutTopic)

  /**creating object of kafkaStreams and starting streaming*/
  val stream: KafkaStreams = new KafkaStreams(builder, getStreamConf)
  stream.start()
}
