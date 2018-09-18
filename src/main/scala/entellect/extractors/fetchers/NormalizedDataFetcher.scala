package entellect.extractors.fetchers

import java.io.ByteArrayOutputStream

import akka.event.Logging
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.Attributes
import akka.stream.alpakka.slick.scaladsl.Slick
import com.esotericsoftware.kryo.io.Output
import entellect.extractors.RawData
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import akka.stream.alpakka.slick.scaladsl._
import akka.stream.scaladsl.{Keep, Merge, Sink, Source}
import entellect.extractors._
import org.apache.kafka.clients.producer.ProducerRecord

import scala.concurrent.Future
import scala.util.Failure


object NormalizedDataFetcher extends App {

  implicit val session = SlickSession.forConfig("pp")

  import ExecutionContext._
  import KryoContext._
  import session.profile.api._
  import SlickQuery._



  val config = system.settings.config.getConfig("akka.kafka.producer")
  val producerSettings =
    ProducerSettings(config, new StringSerializer, new ByteArraySerializer)
      .withBootstrapServers("localhost:9092")

  val tableRows = Source.combine(
    Slick.source(sql"""select * FROM DRUG""".as[(Map[String,String])])
      .map{e => RawData("DRUG", "/Users/maatari/karma/models-autosave/WSP1WS5-DRUG-auto-model.ttl", "OBJECT", "", e)},

    Slick.source(sql"""select * FROM ROUTE""".as[(Map[String,String])])
      .map{e => RawData("ROUTE", "/Users/maatari/karma/models-autosave/WSP1WS5-ROUTE-auto-model.ttl", "OBJECT", "", e)},

      Slick.source(sql"""select * FROM SOURCE""".as[(Map[String,String])])
    .map{e => RawData("SOURCE", "/Users/maatari/karma/models-autosave/WSP1WS5-SOURCE-auto-model.ttl", "OBJECT", "", e)},

    Slick.source(sql"""select * FROM PKDATA""".as[(Map[String,String])])
      .map{e => RawData("PKDATA", "/Users/maatari/karma/models-autosave/WSP1WS5-PKDATA-auto-model.ttl", "OBJECT", "", e)},

    Slick.source(sql"""select * FROM FDADOCUMENT""".as[(Map[String,String])])
      .map{e => RawData("FDADOCUMENT", "/Users/maatari/karma/models-autosave/WSP1WS5-FDADOCUMENT-auto-model.ttl", "OBJECT", "", e)},

    Slick.source(sql"""select * FROM EMEADOCUMENT""".as[(Map[String,String])])
      .map{e => RawData("EMEADOCUMENT", "/Users/maatari/karma/models-autosave/WSP1WS5-EMEADOCUMENT-auto-model.ttl", "OBJECT", "", e)}
  )(Merge(_)).addAttributes(Attributes.inputBuffer(128, 128))
    /*.toMat(Sink.foreach{ e =>})(Keep.right)
    .run()*/
      .mapAsyncUnordered(8){value =>
        Future{
          //println(s"Writing {${value.toString}}")
          val kryo = kryoPool.obtain()
          val outStream = new ByteArrayOutputStream()
          val output = new Output(outStream, 20480)
          kryo.writeClassAndObject(output, value)
          output.close()
          kryoPool.free(kryo)
          new ProducerRecord[String, Array[Byte]]("test", outStream.toByteArray)
        }
      }
      .runWith(Producer.plainSink(producerSettings))


  tableRows.onComplete {
    case Failure(exception) => {println(s"table ended with exception ${exception.toString} "); system.terminate()}
    case _ => system.terminate()
  }

}
