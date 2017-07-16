package kafka.streams.processorAPI

import java.util.Objects

import org.apache.kafka.streams.processor.{AbstractProcessor, ProcessorContext}
import org.apache.kafka.streams.state.KeyValueStore

/**
  * the class where the logic is added to be performed on every key value pair
  * extending AbstractProcessor class to get the overridden processor method
  */
class ProcessorImpl extends AbstractProcessor[String, String]{

  var keyValueStore: KeyValueStore[String, String] = _
  var processorContext: ProcessorContext = _

  override def init(context: ProcessorContext): Unit = {
    processorContext = context
    processorContext.schedule(10000L)
    keyValueStore = processorContext.getStateStore("tester").asInstanceOf[KeyValueStore[String, String]]
    Objects.requireNonNull(keyValueStore, "State Store can't be null")
  }
/**
  * here logic is implemented
  * every value for a key must be greater than the previous value
  * */
  override def process(key: String, value: String): Unit = {
    val oldValue = keyValueStore.get(key)
    if(oldValue == null || oldValue.toInt < value.toInt) {
      println(s"found greater value sending $oldValue<<<<<<<<<$value for key $key")
      //sending key value through
      processorContext.forward(key, value)
      //updating data in local state store
      keyValueStore.put(key, value)
      processorContext.commit()
    } else {
      println(s"found less value  $oldValue>>>>>>>>$value not sending anything")
    }
  }
}

/*

import java.sql.Timestamp
import java.util.{Objects, Properties}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.processor.{AbstractProcessor, ProcessorContext, TopologyBuilder}
import org.apache.kafka.streams.state.{KeyValueIterator, KeyValueStore}
import org.joda.time.{DateTime, LocalDateTime}
import org.joda.time.format.DateTimeFormat

class OrderingProcessor extends AbstractProcessor[String, String]{
  var keyValueStore: KeyValueStore[String, String] = _
  var processorContext: ProcessorContext = _

  val properties = new Properties()
  properties.put("bootstrap.servers", "localhost:9092")
  properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](properties)

  val stringSer = new StringSerializer
  val stringDe = new StringDeserializer

  override def process(key: String, value: String): Unit = {
    val v1 = keyValueStore.get(key)
    if(v1 == null || v1.toInt < value.toInt) {
      println(s"found greater value.>>>>>>>>>>>>>$value")
      keyValueStore.put(key, value)
      processorContext.forward(key, value)
      processorContext.commit()
    }
    else println(s"denied value>>>>>>>>>>>>>$value")
  }

  override def init(context: ProcessorContext): Unit = {
    processorContext = context
    processorContext.schedule(10000L)
    keyValueStore = processorContext.getStateStore("testStore").asInstanceOf[KeyValueStore[String, String]]
    Objects.requireNonNull(keyValueStore, "State Store can't be null")
  }

  override def punctuate(timestamp: Long): Unit = {
    val storeIterator = keyValueStore.all()
    //    while(storeIterator.hasNext){
    //      println(storeIterator.next().value+"this is the value from punctuate")
    //    }
  }
}
*/
