package entellect

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.util.Pool
import com.romix.scala.serialization.kryo.{ScalaImmutableAbstractMapSerializer, ScalaProductSerializer}
import edu.isi.karma.controller.update.UpdateContainer
import edu.isi.karma.er.helper.{PythonRepository, PythonRepositoryRegistry}
import edu.isi.karma.metadata._
import edu.isi.karma.rdf.GenericRDFGenerator
import edu.isi.karma.webserver.ContextParametersRegistry
import edu.isi.karma.webserver.ServletContextParameterMap.ContextParameter

package object extractors {

  case class RawData(modelName: String,
                     modelFile: String,
                     sourceType: String,
                     deNormalizedVal: String,
                     normalVal: Map[String, String])

  object KryoContext {
    lazy val kryoPool = new Pool[Kryo](true, false, 16) {
      protected def create(): Kryo = {
        val kryo = new Kryo()
        kryo.setRegistrationRequired(false)
        kryo.addDefaultSerializer(classOf[scala.collection.Map[_,_]], classOf[ScalaImmutableAbstractMapSerializer])
        kryo.addDefaultSerializer(classOf[scala.collection.generic.MapFactory[scala.collection.Map]], classOf[ScalaImmutableAbstractMapSerializer])
        kryo.addDefaultSerializer(classOf[RawData], classOf[ScalaProductSerializer])
        kryo
      }
    }
  }

  object ExecutionContext {

    implicit lazy val system  = ActorSystem()
    implicit lazy val mat     = ActorMaterializer()
    implicit lazy val ec      = system.dispatcher

  }

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
