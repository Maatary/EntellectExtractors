package entellect.extractors.mappers.transformations

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{JsonFraming, Sink, Source}
import akka.util.ByteString
import edu.isi.karma.rdf.GenericRDFGenerator
import edu.isi.karma.rdf.GenericRDFGenerator.InputType
import edu.isi.karma.webserver.ContextParametersRegistry
import entellect.extractors.RawData
import org.apache.spark.sql.Row
import entellect.extractors.mappers.karma._

import scala.concurrent.ExecutionContextExecutor



object DataTransformationService {

  /*def mapRawDataToRowRdf(rdfGenerator: GenericRDFGenerator, rawDataList: List[RawData], contextParametersRegistry: ContextParametersRegistry): List[Row] = {

    val headers = rawDataList.head.normalVal.toList.unzip._1
    val values = rawDataList.map(rawData => rawData.normalVal.toList.unzip._2)
    val modelName = rawDataList.head.modelName
    val modelFile = rawDataList.head.modelFile

    import KarmaUtilService._
    val modelIdentifier = getR2RMLMappingIdentifier(modelName, modelFile)
    rdfGenerator.addModel(modelIdentifier)
    val writer = getRDFWriter(rdfGenerator, modelName)
    val request = getRDFGeneratorRequest(writer._1, headers, values, modelName, modelName, ContextParametersRegistry.getInstance().getDefault)
    rdfGenerator.generateRDF(request)
    List(Row(writer._2.toString))
  }*/

  def mapRawDataToRowRdf(rdfGenerator: GenericRDFGenerator,
                          rawDataList: ((String, String), List[RawData]), inputType: InputType,
                          contextParametersRegistry: ContextParametersRegistry)
                         (implicit system: ActorSystem, mat: ActorMaterializer, ec: ExecutionContextExecutor) = {

    val headers   = rawDataList._2.head.normalVal.toList.unzip._1
    val values    = rawDataList._2.map(rawData => rawData.normalVal.toList.unzip._2)
    val modelFile = rawDataList._2.head.modelFile
    val modelName = rawDataList._1._1

    import KarmaUtilService._
    val modelIdentifier = getR2RMLMappingIdentifier(modelName, modelFile)
    rdfGenerator.addModel(modelIdentifier)
    val writer = getRDFWriter(rdfGenerator, modelName)
    val request = getRDFGeneratorRequest(writer._1, headers, values, modelName, modelName, inputType, contextParametersRegistry.getDefault)
    rdfGenerator.generateRDF(request)
    val done = Source
      .single(ByteString(writer._2.toString))
      .via(JsonFraming.objectScanner(100000))
      .map(s => Row(s.utf8String)).runWith(Sink.seq)
    List(done)
  }

}
