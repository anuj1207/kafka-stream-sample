package kafka.streams.processorAPI

import java.util.Properties

import org.apache.kafka.common.serialization.{Serde, Serdes, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.processor.{Processor, ProcessorSupplier}
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

object ProcessorApplication extends App{
  val streamsConfiguration = new Properties()
  streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "Streaming-QuickStart")
  streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
  streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)

  val stringSer = new StringSerializer
  val stringDe = new StringDeserializer
  val stringSerde: Serde[String] = Serdes.String()
  val inTopic = "topic"
  val builder = new KStreamBuilder

  builder
    .addSource("source1", stringDe, stringDe, inTopic)//adding source topic
    //now adding processor class using ProcessSupplier
    .addProcessor("order", new ProcessorSupplier[String, String] {
      override def get(): Processor[String, String] = new ProcessorImpl
    }, "source1")
    //adding local state store for stateful operations
    .addStateStore(Stores.create("tester").withStringKeys.withStringValues.inMemory.build, "order")
    //adding destination topic for the processed data to go
    .addSink("sink", "out", stringSer, stringSer, "order")

  //creating KafkaStreams object and starting streaming
  val stream = new KafkaStreams(builder, streamsConfiguration)
  stream.start()
}
