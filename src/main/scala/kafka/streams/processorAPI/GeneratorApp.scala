package kafka.streams.processorAPI

import java.util.Properties

import akka.actor.ActorSystem
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps

object GeneratorApp extends App{
  val properties = new Properties()

  properties.put("bootstrap.servers", "localhost:9092")
  properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer = new KafkaProducer[String, String](properties)

  var ctr = 1
  var value = 0
  val system = ActorSystem("system")
  system.scheduler.schedule(0 second, 2 seconds){
    if(ctr % 6 == 0 ) value = value - 1
    else value = value + 1
    val record1 = new ProducerRecord[String, String]("topic","key", value.toString)
    println("sending ctr = " + ctr + "   " + value)
    ctr += 1
    producer.send(record1)
  }
}
