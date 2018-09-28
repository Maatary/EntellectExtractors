package entellect.extractors.fetchers

import java.io.{ByteArrayOutputStream, File}

import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.scaladsl.{FileIO, JsonFraming, Keep, Sink}
import com.esotericsoftware.kryo.io.Output

import scala.concurrent.Future
import scala.util.{Failure, Success}
import com.elsevier.entellect.commons._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

object DeNormalizedDataFetcherMock extends App {

  import ExecutionContext._
  import KryoContext._


  val config = system.settings.config.getConfig("akka.kafka.producer")
  val producerSettings =
    ProducerSettings(config, new StringSerializer, new ByteArraySerializer)
      .withBootstrapServers("localhost:9092")

  val fileStream      = FileIO.fromPath(new File("/Users/maatari/IdeaProjects/OpenTarget-json_schema/18.06_evidence_data.json").toPath)
  val jsonFramingFlow = JsonFraming.objectScanner(1000000)

  val done = fileStream.async.viaMat(jsonFramingFlow)(Keep.both)
    .mapAsyncUnordered(2){ e => Future{e.utf8String}}
    .map{e => RawData("SampleEvidenceExpression", "/Users/maatari/karma/models-autosave/WSP1WS734-SampleEvidenceExpression.json-auto-model.ttl", "JL", e, null)}
    .map{value =>
      val kryo = kryoPool.obtain()
      val outStream = new ByteArrayOutputStream()
      val output = new Output(outStream, 8192)
      kryo.writeClassAndObject(output, value)
      output.close()
      kryoPool.free(kryo)
      new ProducerRecord[String, Array[Byte]]("DeNormalizedRawData", outStream.toByteArray)
    }
    .runWith(Producer.plainSink(producerSettings))

  done.onComplete{
    case Failure(e) => println(s"Stream failure with exception: ${e.toString}"); system.terminate()
    case _ => println(s"Stream ended in success"); system.terminate()
  }

}
