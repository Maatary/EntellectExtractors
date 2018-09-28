package entellect

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer

import edu.isi.karma.controller.update.UpdateContainer
import edu.isi.karma.er.helper.{PythonRepository, PythonRepositoryRegistry}
import edu.isi.karma.metadata._
import edu.isi.karma.rdf.GenericRDFGenerator
import edu.isi.karma.webserver.ContextParametersRegistry
import edu.isi.karma.webserver.ServletContextParameterMap.ContextParameter

package object extractors {



  object KarmaContext {

    lazy val context = {

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
      contextParametersRegistry

    }

    lazy val rdfGenerator = new GenericRDFGenerator
  }

}
