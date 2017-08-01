package kafka.streams.processorAPI

import com.madewithtea.mockedstreams.MockedStreams
import kafka.streams.TestConstants._
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class StreamProcessorSpec extends FlatSpec with BeforeAndAfterAll {
  var streamProcessor: StreamProcessor = _
  var builder: KStreamBuilder = _

  val rightSeq = Seq(("key", "1"), ("key", "2"), ("key", "3"), ("key", "4"), ("key", "5"), ("key", "6"), ("key", "7"))
  val wrongSeq = Seq(("key", "1"), ("key", "4"), ("key", "3"), ("key", "4"), ("key", "5"), ("key", "6"), ("key", "7"))

  override protected def beforeAll(): Unit = {
    builder = new KStreamBuilder
    streamProcessor = new StreamProcessor
  }

  it should "verify and accept input stream" in {
    val res = MockedStreams().topology{builder =>
      streamProcessor.orderStreamData(builder, firstInTopic, firstOutTopic)
    }.config(getStreamConf)
      .input(firstInTopic, stringSerde, stringSerde, rightSeq)
      .output(firstOutTopic, stringSerde, stringSerde, rightSeq.size)
    println(res)
    assert(res.toList === rightSeq)
  }

  it should "filter unordered records " in {
    val result = MockedStreams().topology{builder =>
      streamProcessor.orderStreamData(builder, firstInTopic, firstOutTopic)
    }.config(getStreamConf)
      .input(firstInTopic, stringSerde, stringSerde, wrongSeq)
      .output(firstOutTopic, stringSerde, stringSerde, wrongSeq.size)
    println(s"$result ::size of filtered output is " + result.size)
    assert(result.size != wrongSeq.size)
  }

}
