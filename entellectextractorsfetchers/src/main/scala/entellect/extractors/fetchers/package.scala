package entellect.extractors

import java.io.File

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{FileIO, JsonFraming, Keep, Sink}

import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.concurrent.duration.Duration
import spray.json._

package object fetchers {

  case class Drug(ID: Int,
                  STRUCTUREID: Option[String],
                  MOLFILE: Option[String],
                  MOLWEIGHT: Option[Double],
                  FORMULA: Option[String],
                  NAME: Option[String],
                  HASH: Option[String],
                  SMILES: Option[String],
                  METABOLISM: Option[String])

  object DrugJsonProtocol extends DefaultJsonProtocol {
    implicit val colorFormat = jsonFormat9(Drug)
  }


  object DrugUtilService {

    import DrugJsonProtocol._

    def DrugToMap (drug: Drug) : Map[String, String] = {
      Map("ID" -> drug.ID.toString,
        "STRUCTUREID" -> drug.STRUCTUREID.getOrElse(""),
        "MOLFILE" -> drug.MOLFILE.getOrElse(""),
        "MOLWEIGHT" -> drug.MOLWEIGHT.getOrElse("").toString,
        "FORMULA" -> drug.FORMULA.getOrElse(""),
        "NAME" -> drug.NAME.getOrElse(""),
        "HASH" -> drug.HASH.getOrElse(""),
        "SMILES" -> drug.SMILES.getOrElse(""),
        "METABOLISM" -> drug.METABOLISM.getOrElse("")
      )
    }

    def loadDrugsAsMaps(filename: String)
                       (implicit system: ActorSystem, mat: ActorMaterializer, ec: ExecutionContextExecutor) = {

      val fileStream      = FileIO.fromPath(new File(filename).toPath)
      val jsonFramingFlow = JsonFraming.objectScanner(100000)

      val rows = fileStream.async.viaMat(jsonFramingFlow)(Keep.right)
        .mapAsyncUnordered(16){ e => Future{DrugToMap(e.utf8String.parseJson.convertTo[Drug])}}
        .toMat(Sink.seq)(Keep.right).run()
      Await.result(rows, Duration.Inf)
    }

    def drugsSource(filename: String)
                   (implicit system: ActorSystem, mat: ActorMaterializer, ec: ExecutionContextExecutor) = {

      val fileStream      = FileIO.fromPath(new File(filename).toPath)
      val jsonFramingFlow = JsonFraming.objectScanner(100000)

      val rows = fileStream.async.viaMat(jsonFramingFlow)(Keep.left)
        .mapAsyncUnordered(8){ e => Future{DrugToMap(e.utf8String.parseJson.convertTo[Drug])}}
      rows
    }

  }

}
