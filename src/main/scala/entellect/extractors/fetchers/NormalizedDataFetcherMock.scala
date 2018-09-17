package entellect.extractors.fetchers

import entellect.extractors._
import java.io.ByteArrayOutputStream

import akka.Done
import akka.kafka.ProducerSettings
import akka.kafka.scaladsl.Producer
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.util.Pool
import com.romix.scala.serialization.kryo.{ScalaImmutableAbstractMapSerializer, ScalaProductSerializer}
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}

import scala.concurrent.Future
import scala.util.Failure



object NormalizedDataFetcherMock extends App {

  import ExecutionContext._

  val config = system.settings.config.getConfig("akka.kafka.producer")
  val producerSettings =
    ProducerSettings(config, new StringSerializer, new ByteArraySerializer)
      .withBootstrapServers("localhost:9092")

  val kryoPool = new Pool[Kryo](true, false, 16) {
    protected def create(): Kryo = {
      val kryo = new Kryo()
      kryo.setRegistrationRequired(false)
      kryo.addDefaultSerializer(classOf[scala.collection.Map[_,_]], classOf[ScalaImmutableAbstractMapSerializer])
      kryo.addDefaultSerializer(classOf[scala.collection.generic.MapFactory[scala.collection.Map]], classOf[ScalaImmutableAbstractMapSerializer])
      kryo.addDefaultSerializer(classOf[RawData], classOf[ScalaProductSerializer])
      kryo
    }
  }

  import DrugUtilService._

  val done: Future[Done] =
    drugsSource("/Users/maatari/Desktop/PHARMAPENDIUM4_DRUG.json")
      .map{e => RawData("DRUG", "/Users/maatari/karma/models-autosave/WSP1WS5-DRUG-auto-model.ttl", "Json", "", e)}
      .map{value =>
        println(s"Writing {${value.toString}}")
        val kryo = kryoPool.obtain()
        val outStream = new ByteArrayOutputStream()
        val output = new Output(outStream, 4096)
        kryo.writeClassAndObject(output, value)
        output.close()
        kryoPool.free(kryo)
        new ProducerRecord[String, Array[Byte]]("test", outStream.toByteArray)
      }
      .runWith(Producer.plainSink(producerSettings))

  done.onComplete{
    case Failure(e) => s"Stream failer with exception: ${e.toString}"; system.terminate()
    case _ => system.terminate()
  }
}
