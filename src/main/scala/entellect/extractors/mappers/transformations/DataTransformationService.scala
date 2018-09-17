package entellect.extractors.mappers.transformations

import edu.isi.karma.rdf.GenericRDFGenerator
import edu.isi.karma.webserver.ContextParametersRegistry
import entellect.extractors.RawData
import org.apache.spark.sql.Row
import entellect.extractors.mappers.karma._



object DataTransformationService {

  def mapRawDataToRowRdf(rdfGenerator: GenericRDFGenerator, rawDataList: List[RawData], contextParametersRegistry: ContextParametersRegistry): List[Row] = {

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
  }
}
