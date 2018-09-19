package entellect.extractors.consumers

import java.nio.file.Paths

import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.stream.scaladsl.{FileIO, Keep}
import akka.util.ByteString
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import entellect.extractors._

import scala.concurrent.Future
import scala.util.Failure
object SourceRDFConsumerMock extends App {


  import ExecutionContext._

  val config = system.settings.config.getConfig("akka.kafka.consumer")
  val consumerSettings =
    ConsumerSettings(config, new StringDeserializer, new StringDeserializer)
      .withBootstrapServers("localhost:9092")
      .withGroupId("group1")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val control =
    Consumer
      .plainSource(consumerSettings, Subscriptions.topics("NormalizedSourceRDF"))
      .mapAsyncUnordered(1){cr =>
        println("got a message to write")
        Future{ByteString(cr.value())}
      }
      .toMat(FileIO.toPath(Paths.get("pp.json-ld")))(Keep.both)
      .run()


  control._2.onComplete{
    case Failure(exception) => {println(s"Write ended with exception ${exception.toString} "); system.terminate()}
    case scala.util.Success(value) => println(value) ; system.terminate()
  }

}
