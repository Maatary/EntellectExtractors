package entellect.extractors.mappers.jena

import com.hp.hpl.jena.rdf.model.Model
import edu.isi.karma.modeling.Uris
import scala.collection.JavaConversions._

object JenaOperationService {

  def extractBaseURI (model: Model): String = {
    var baseURI = ""
    val rdfTypeProp = model.getProperty(Uris.RDF_TYPE_URI)
    val baseURIProp = model.getProperty(Uris.KM_HAS_BASEURI)
    val node = model.getResource(Uris.KM_R2RML_MAPPING_URI)
    val res = model.listResourcesWithProperty(rdfTypeProp, node)
    val resList = res.toList

    for (r <- resList) {
      if (r.hasProperty(baseURIProp)) {
        baseURI = r.getProperty(baseURIProp).asTriple.getObject.toString
        baseURI = baseURI.replace("\"", "")
      }
    }
    baseURI
  }
}
