package entellect.extractors.mappers.transformations

import java.io.{ByteArrayInputStream, PrintWriter, StringWriter}

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
import org.apache.jena.rdf.model.{ModelFactory, RDFNode, SimpleSelector}

import scala.concurrent.ExecutionContextExecutor
import scala.collection.JavaConverters._




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

  def mapRawNormalizedDataToRowRdf(rdfGenerator: GenericRDFGenerator,
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


  def mapRawDeNormalizedDataToRowRdf(rdfGenerator: GenericRDFGenerator,
                                     rawDataList: ((String, String), List[RawData]), inputType: InputType,
                                     contextParametersRegistry: ContextParametersRegistry)
                                     (implicit system: ActorSystem, mat: ActorMaterializer, ec: ExecutionContextExecutor) = {

    val modelFile = rawDataList._2.head.modelFile
    val modelName = rawDataList._1._1

    val denormalizedVal = rawDataList._2.map(rd => rd.deNormalizedVal).mkString("\n")

    import KarmaUtilService._
    val modelIdentifier = getR2RMLMappingIdentifier(modelName, modelFile)
    rdfGenerator.addModel(modelIdentifier)
    val writer = getRDFWriter(rdfGenerator, modelName)
    val request = getRDFGeneratorRequest(writer._1, denormalizedVal, modelName, modelName, inputType, contextParametersRegistry.getDefault)
    rdfGenerator.generateRDF(request)
    val done = Source
      .single(ByteString(writer._2.toString))
      .via(JsonFraming.objectScanner(100000))
      .map(s => s.utf8String).mapConcat(splitInIdEntity(_))
      .runWith(Sink.seq)
    List(done)
  }


  def splitInIdEntity(denormalizedEntity: String) = {

    val inputStream = new ByteArrayInputStream(denormalizedEntity.getBytes)
    val model       = ModelFactory.createDefaultModel()
    val s           = new SimpleSelector(null, null, null.asInstanceOf[RDFNode])
    val it          = model.read(inputStream, null, "JSON-LD").listStatements(s)
    val stmts       = it.asScala.toList
    val entities    = stmts.groupBy(e => e.getSubject.toString)

    val split       = entities.map { e =>
      val model = ModelFactory.createDefaultModel()
      model.add(e._2.asJava)
      val sw              = new StringWriter
      val pw              = new PrintWriter(sw)
      model.write(pw, "JSON-LD")
      pw.close()
      model.close()
      Row(e._1, sw.toString)
    }.toList

    model.close()

    split
  }



}
