package entellect.extractors.fetchers

import java.io.ByteArrayOutputStream

import akka.event.Logging
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import akka.stream.Attributes
import akka.stream.alpakka.slick.scaladsl.Slick
import com.esotericsoftware.kryo.io.Output
import com.elsevier.entellect.commons._
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import akka.stream.alpakka.slick.scaladsl._
import akka.stream.scaladsl.{Keep, Merge, Sink, Source}
import entellect.extractors._
import org.apache.kafka.clients.producer.ProducerRecord
import slick.jdbc.{ResultSetConcurrency, ResultSetType}

import scala.concurrent.Future
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}
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

  val start = FiniteDuration(System.currentTimeMillis(), MILLISECONDS)

  val tableRows = Source.combine(
    Slick.source(sql"""select * FROM DRUG""".as[(Map[String,String])]
      .withStatementParameters(rsType = ResultSetType.ForwardOnly, rsConcurrency = ResultSetConcurrency.ReadOnly, fetchSize = 10000))
      .map{e => RawData("DRUG", "/Users/maatari/karma/models-autosave/WSP1WS5-DRUG-auto-model.ttl", "OBJECT", "", e)},

    Slick.source(sql"""select * FROM ROUTE""".as[(Map[String,String])]
      .withStatementParameters(rsType = ResultSetType.ForwardOnly, rsConcurrency = ResultSetConcurrency.ReadOnly, fetchSize = 10000))
      .map{e => RawData("ROUTE", "/Users/maatari/karma/models-autosave/WSP1WS5-ROUTE-auto-model.ttl", "OBJECT", "", e)},

    Slick.source(sql"""select * FROM SOURCE""".as[(Map[String,String])]
      .withStatementParameters(rsType = ResultSetType.ForwardOnly, rsConcurrency = ResultSetConcurrency.ReadOnly, fetchSize = 10000))
      .map{e => RawData("SOURCE", "/Users/maatari/karma/models-autosave/WSP1WS5-SOURCE-auto-model.ttl", "OBJECT", "", e)},

    Slick.source(sql"""select * FROM PKDATA""".as[(Map[String,String])]
      .withStatementParameters(rsType = ResultSetType.ForwardOnly, rsConcurrency = ResultSetConcurrency.ReadOnly, fetchSize = 10000))
      .map{e => RawData("PKDATA", "/Users/maatari/karma/models-autosave/WSP1WS5-PKDATA-auto-model.ttl", "OBJECT", "", e)},

    Slick.source(sql"""select * FROM FDADOCUMENT""".as[(Map[String,String])]
      .withStatementParameters(rsType = ResultSetType.ForwardOnly, rsConcurrency = ResultSetConcurrency.ReadOnly, fetchSize = 10000))
      .map{e => RawData("FDADOCUMENT", "/Users/maatari/karma/models-autosave/WSP1WS5-FDADOCUMENT-auto-model.ttl", "OBJECT", "", e)},

    Slick.source(sql"""select * FROM EMEADOCUMENT""".as[(Map[String,String])]
      .withStatementParameters(rsType = ResultSetType.ForwardOnly, rsConcurrency = ResultSetConcurrency.ReadOnly, fetchSize = 10000))
      .map{e => RawData("EMEADOCUMENT", "/Users/maatari/karma/models-autosave/WSP1WS5-EMEADOCUMENT-auto-model.ttl", "OBJECT", "", e)},

    Slick.source(sql"""select * FROM EMEATYPE""".as[(Map[String,String])]
      .withStatementParameters(rsType = ResultSetType.ForwardOnly, rsConcurrency = ResultSetConcurrency.ReadOnly, fetchSize = 10000))
      .map{e => RawData("EMEATYPE", "/Users/maatari/karma/models-autosave/WSP1WS5-EMEATYPE-auto-model.ttl", "OBJECT", "", e)},

    Slick.source(sql"""select * FROM FDATYPE""".as[(Map[String,String])]
      .withStatementParameters(rsType = ResultSetType.ForwardOnly, rsConcurrency = ResultSetConcurrency.ReadOnly, fetchSize = 10000))
      .map{e => RawData("FDATYPE", "/Users/maatari/karma/models-autosave/WSP1WS5-FDATYPE-auto-model.ttl", "OBJECT", "", e)},

    Slick.source(sql"""select * FROM PKPARAMETER""".as[(Map[String,String])]
      .withStatementParameters(rsType = ResultSetType.ForwardOnly, rsConcurrency = ResultSetConcurrency.ReadOnly, fetchSize = 10000))
      .map{e => RawData("PKPARAMETER", "/Users/maatari/karma/models-autosave/WSP1WS5-PKPARAMETER-auto-model.ttl", "OBJECT", "", e)},

    Slick.source(sql"""select * FROM SOURCEHIERARCHY""".as[(Map[String,String])]
      .withStatementParameters(rsType = ResultSetType.ForwardOnly, rsConcurrency = ResultSetConcurrency.ReadOnly, fetchSize = 10000))
      .map{e => RawData("SOURCEHIERARCHY", "/Users/maatari/karma/models-autosave/WSP1WS5-SOURCEHIERARCHY-auto-model.ttl", "OBJECT", "", e)},

    Slick.source(sql"""select * FROM SPECIE""".as[(Map[String,String])]
      .withStatementParameters(rsType = ResultSetType.ForwardOnly, rsConcurrency = ResultSetConcurrency.ReadOnly, fetchSize = 10000))
      .map{e => RawData("SPECIE", "/Users/maatari/karma/models-autosave/WSP1WS5-SPECIE-auto-model.ttl", "OBJECT", "", e)}

  )(Merge(_))//.addAttributes(Attributes.inputBuffer(1024, 1024))
    .mapAsyncUnordered(8){value =>
      Future{
        val kryo = kryoPool.obtain()
        val outStream = new ByteArrayOutputStream()
        val output = new Output(outStream, 4096)
        kryo.writeClassAndObject(output, value)
        output.close()
        kryoPool.free(kryo)
        new ProducerRecord[String, Array[Byte]]("NormalizedRawData", outStream.toByteArray)
        }
      }//.addAttributes(Attributes.inputBuffer(1024, 1024))
      .runWith(Producer.plainSink(producerSettings))


  tableRows.onComplete {
    case Failure(exception) => {println(s"table ended with exception ${exception.toString} "); system.terminate()}
    case _ => val time = (FiniteDuration(System.currentTimeMillis(), MILLISECONDS) - start).toSeconds; println(s"finished in ${time}"); system.terminate()
  }


}
