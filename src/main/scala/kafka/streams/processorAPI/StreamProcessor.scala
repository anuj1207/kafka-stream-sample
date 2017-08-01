package kafka.streams.processorAPI

import kafka.streams.Constants._
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.apache.kafka.streams.processor.{Processor, ProcessorSupplier, TopologyBuilder}
import org.apache.kafka.streams.state.Stores

class StreamProcessor {

  def orderStreamData(builder: KStreamBuilder, inTopic: String, outTopic: String): TopologyBuilder = {
    builder
      .addSource("source1", stringDe, stringDe, inTopic)//adding source topic
      //now adding processor class using ProcessSupplier
      .addProcessor("order", new ProcessorSupplier[String, String] {
      override def get(): Processor[String, String] = new ProcessorImpl
    }, "source1")
      //adding local state store for stateful operations
      .addStateStore(Stores.create("tester").withStringKeys.withStringValues.inMemory.build, "order")
      //adding destination topic for the processed data to go
      .addSink("sink", outTopic, stringSer, stringSer, "order")
  }

}
