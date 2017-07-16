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
    //accessing local state store for last value saved for this key
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
