package kafka.streams.dsl

import org.apache.kafka.streams.kstream.{JoinWindows, KStream, KStreamBuilder, ValueJoiner}

class StreamOperations {

  /**joins data from two topics and returns a KStream */
  def join(builder: KStreamBuilder, topic1: String, topic2: String, window: Long): KStream[String, String] = {
    val stream1: KStream[String, String] = builder.stream(topic1)
    val stream2: KStream[String, String] = builder.stream(topic2)

    /** Joining streams subscribed to two different topics
    * the data will be joined according to the apply method of ValueJoiner class*/
    val abc: KStream[String, String] = stream1.join(
        stream2,
        new ValueJoiner[String, String, String] {
          override def apply(v1: String, v2: String): String = v1+v2
        },
        JoinWindows.of(window)
      )
    abc
  }

  /**converts characters to upper case*/
  def toUpperCase(builder: KStreamBuilder, inTopic: String): KStream[String, String] = {
    val stream: KStream[String, String] = builder.stream(inTopic)

    /** Data receiving from inTopic
      * converting to upper case characters */
    stream.mapValues[String](_.toUpperCase)
  }
}
