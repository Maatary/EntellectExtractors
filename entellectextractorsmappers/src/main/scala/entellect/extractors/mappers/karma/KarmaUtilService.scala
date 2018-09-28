package entellect.extractors.mappers.karma

import java.io.{File, PrintWriter, StringWriter}

import edu.isi.karma.kr2rml.ContextGenerator
import edu.isi.karma.kr2rml.mapping.R2RMLMappingIdentifier
import edu.isi.karma.kr2rml.writer.{JSONKR2RMLRDFWriter, KR2RMLRDFWriter}
import edu.isi.karma.rdf.GenericRDFGenerator.InputType
import edu.isi.karma.rdf.{GenericRDFGenerator, RDFGeneratorInputWrapper, RDFGeneratorRequest}
import edu.isi.karma.webserver.ServletContextParameterMap
import entellect.extractors.mappers.jena._

import scala.collection.JavaConverters._


object KarmaUtilService {

  def getR2RMLMappingIdentifier (modelName: String, modelFile: String) : R2RMLMappingIdentifier = {
    new R2RMLMappingIdentifier(modelName, new File(modelFile).toURI.toURL)
  }
  def getRDFWriter(rdfGenerator: GenericRDFGenerator, modelName: String): (KR2RMLRDFWriter, StringWriter) = {
    val sw              = new StringWriter
    val pw              = new PrintWriter(sw)
    val model           = rdfGenerator.getModelParser(modelName).getModel
    val writer          = new JSONKR2RMLRDFWriter(pw, JenaOperationService.extractBaseURI(model))
    writer.setGlobalContext(new ContextGenerator(model, true).generateContext, null)
    (writer, sw)
  }


  def getRDFGeneratorRequest(writer: KR2RMLRDFWriter,
                             headers: List[String],
                             values: List[List[String]],
                             modelName: String,
                             sourceName: String,
                             inputType: InputType,
                             contexMap: ServletContextParameterMap): RDFGeneratorRequest = {

    val wrapper         = new RDFGeneratorInputWrapper(headers.asJava, values.map(_.asJava).asJava)
    val request         = new RDFGeneratorRequest(modelName, sourceName)
    request.setInput(wrapper)
    request.setAddProvenance(false)
    request.setDataType(inputType)
    request.addWriter(writer)
    request.setContextParameters(contexMap)
    request
  }

  def getRDFGeneratorRequest(writer: KR2RMLRDFWriter,
                             denormalizedVal: String,
                             modelName: String,
                             sourceName: String,
                             inputType: InputType,
                             contexMap: ServletContextParameterMap): RDFGeneratorRequest = {

    val wrapper         = new RDFGeneratorInputWrapper(denormalizedVal)
    val request         = new RDFGeneratorRequest(modelName, sourceName)
    request.setInput(wrapper)
    request.setAddProvenance(false)
    request.setDataType(inputType)
    request.addWriter(writer)
    request.setContextParameters(contexMap)
    request
  }


}
