package kafka.streams.dsl

import com.madewithtea.mockedstreams.MockedStreams
import kafka.streams.TestConstants
import kafka.streams.TestConstants.stringSerde
import org.apache.kafka.streams.kstream.KStreamBuilder
import org.scalatest.{BeforeAndAfterAll, FlatSpec}

class StreamOperationsSpec extends FlatSpec with BeforeAndAfterAll {
  var streamOperations: StreamOperations = _
  var builder: KStreamBuilder = _

  override protected def beforeAll(): Unit = {
    builder = new KStreamBuilder
    streamOperations = new StreamOperations
  }

  it should "join two KStreams" in {

    val res = MockedStreams().topology { builder =>
      streamOperations.join(builder, "first", "second", 100L).through("sample").to("third")
    }.config(TestConstants.getStreamConf)
      .input[String, String]("first", stringSerde, stringSerde, Seq(("key", "value1"), ("key1", "value2")))
      .input[String, String]("second", stringSerde, stringSerde, Seq(("key", "value11"), ("key1", "value21")))
      .output[String, String]("third", stringSerde, stringSerde, 2)
    println(res)
    val exp = Seq(("key", "value1value11"), ("key1", "value2value21")/*, ("key", "value11"), ("key", "value21")*/)
    assert(res.toList === exp)
  }

  it should " change characters to upper case" in {
    val res = MockedStreams().topology{builder =>
      streamOperations.toUpperCase(builder, "1").to("2")
    }.config(TestConstants.getStreamConf)
      .input[String, String]("1", stringSerde, stringSerde, Seq(("key", "value1"), ("key", "value2")))
      .output[String, String]("2", stringSerde, stringSerde, 2)
    val exp = Seq(("key", "VALUE1"), ("key", "VALUE2"))
    println(res)
    assert(res.toList == exp)
  }

  override protected def afterAll(): Unit = {
  }
}
