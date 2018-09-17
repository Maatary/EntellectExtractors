package entellect

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.util.Pool
import com.romix.scala.serialization.kryo.{ScalaImmutableAbstractMapSerializer, ScalaProductSerializer}

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

}
