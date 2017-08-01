package old.kafka.streams.dsl

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
  val system = ActorSystem("system")
  system.scheduler.schedule(0 second, 2 seconds){
    val record1 = new ProducerRecord[String, String]("topic-1",ctr.toString, "data from topic1 "+ctr)
    val record2 = new ProducerRecord[String, String]("topic-2",ctr.toString, "data from topic2 "+ctr)
    ctr += 1
    producer.send(record1)
    Thread.sleep(990)
    producer.send(record2)
  }
}
