package entellect.extractors.mappers

import entellect.extractors._

import java.io.{ByteArrayInputStream, File, PrintWriter, StringWriter}
import com.esotericsoftware.kryo.io.Input

import com.hp.hpl.jena.rdf.model.Model
import edu.isi.karma.controller.update.UpdateContainer
import edu.isi.karma.er.helper.{PythonRepository, PythonRepositoryRegistry}
import edu.isi.karma.kr2rml.ContextGenerator
import edu.isi.karma.kr2rml.mapping.R2RMLMappingIdentifier
import edu.isi.karma.kr2rml.writer.{JSONKR2RMLRDFWriter, KR2RMLRDFWriter}
import edu.isi.karma.metadata._
import edu.isi.karma.modeling.Uris
import edu.isi.karma.rdf.GenericRDFGenerator.InputType
import edu.isi.karma.rdf.{GenericRDFGenerator, RDFGeneratorInputWrapper, RDFGeneratorRequest}
import edu.isi.karma.webserver.ServletContextParameterMap.ContextParameter
import edu.isi.karma.webserver.{ContextParametersRegistry, ServletContextParameterMap}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import scala.collection.JavaConverters._
import decoder._

object KarmaContext {
  lazy val startContext = {

    val contextParametersRegistry = ContextParametersRegistry.getInstance
    var contextParameters         = contextParametersRegistry.registerByKarmaHome(null)

    val userMetadataManager       = new KarmaMetadataManager(contextParameters)
    val uc                        = new UpdateContainer
    userMetadataManager.register(new UserPreferencesMetadata(contextParameters), uc)
    userMetadataManager.register(new UserConfigMetadata(contextParameters), uc)
    userMetadataManager.register(new PythonTransformationMetadata(contextParameters), uc)
    userMetadataManager.register(new OntologyMetadata(contextParameters), uc)
    val pythonRepository          = new PythonRepository(false, contextParameters.getParameterValue(ContextParameter.USER_PYTHON_SCRIPTS_DIRECTORY))
    PythonRepositoryRegistry.getInstance.register(pythonRepository)
  }
  lazy val rdfGenerator = new GenericRDFGenerator
}


object JenaMModelOperationService {
  def extractBaseURI (model: Model): String = {

    var baseURI = ""
    val rdfTypeProp = model.getProperty(Uris.RDF_TYPE_URI)
    val baseURIProp = model.getProperty(Uris.KM_HAS_BASEURI)
    val node = model.getResource(Uris.KM_R2RML_MAPPING_URI)
    val res = model.listResourcesWithProperty(rdfTypeProp, node)
    val resList = res.toList
    import scala.collection.JavaConversions._
    for (r <- resList) {
      if (r.hasProperty(baseURIProp)) {
        baseURI = r.getProperty(baseURIProp).asTriple.getObject.toString
        baseURI = baseURI.replace("\"", "")
      }
    }
    baseURI
  }
}

object KarmaServiceProvider {

  def getR2RMLMappingIdentifier:R2RMLMappingIdentifier = ???
  def getRDFWriter(rdfGenerator: GenericRDFGenerator, modelName: String): (KR2RMLRDFWriter, StringWriter) = {
    val sw              = new StringWriter
    val pw              = new PrintWriter(sw)
    val model           = rdfGenerator.getModelParser(modelName).getModel
    val writer          = new JSONKR2RMLRDFWriter(pw, JenaMModelOperationService.extractBaseURI(model))
    writer.setGlobalContext(new ContextGenerator(model, true).generateContext, null)
    (writer, sw)
  }
  def getRDFGeneratorRequest(writer: KR2RMLRDFWriter,
                             headers: List[String],
                             values: List[List[String]],
                             modelName: String,
                             sourceName: String,
                             contexMap: ServletContextParameterMap): RDFGeneratorRequest = {

    val wrapper         = new RDFGeneratorInputWrapper(headers.asJava, values.map(_.asJava).asJava)
    val request         = new RDFGeneratorRequest(modelName, sourceName)
    request.setInput(wrapper)
    request.setAddProvenance(false)
    request.setDataType(InputType.OBJECT)
    request.addWriter(writer)
    request.setContextParameters(contexMap)
    request
  }

}

object NormalizedDataMapper extends App {

  val spark        = SparkSession.builder()
    .appName("SourceConvertor")
    .master("local[*]")
    .config("spark.executor.memory", "8G")
    .getOrCreate()

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("subscribe", "test")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", false)
    .load().selectExpr("value as message")

  val rdf = df.mapPartitions{ binaryDfIt => {

    if (binaryDfIt.hasNext) {

      import KryoContext._
      import DataDecoderService._


      val rawDataList = decodeData(binaryDfIt.toList, kryoPool)
      val headers   = rawDataList.head.normalVal.toList.unzip._1
      val values    = rawDataList.map(rawData => rawData.normalVal.toList.unzip._2)
      val modelName = rawDataList.head.modelName
      val modelFile = rawDataList.head.modelFile

      import KarmaContext._
      startContext
      import KarmaServiceProvider._
      val modelIdentifier = new R2RMLMappingIdentifier(modelName, new File(modelFile).toURI.toURL)
      rdfGenerator.addModel(modelIdentifier)
      val writer = getRDFWriter(rdfGenerator, modelName)
      val request = getRDFGeneratorRequest(writer._1, headers, values, modelName, modelName, ContextParametersRegistry.getInstance.getDefault)
      rdfGenerator.generateRDF(request)
      Iterator(Row(writer._2.toString))
    }else
      Iterator[Row]()
  }
  }(RowEncoder.apply(StructType(List(StructField("blob", StringType, false)))))



  rdf.repartition(1).writeStream.
    trigger(Trigger.ProcessingTime("1 seconds"))
    .format("text").outputMode("append")
    .option("path", "outputs/drugs")
    .option("checkpointLocation","sparkoutputs/checkpoints")
    .queryName("Drugs").start().awaitTermination()

}