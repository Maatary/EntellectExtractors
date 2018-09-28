package entellect.extractors.mappers.decoder

import java.io.ByteArrayInputStream

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.util.Pool
import com.elsevier.entellect.commons._
import org.apache.spark.sql.Row

object DataDecoderService {


  def decodeData(rowOfBinaryList: List[Row], kryoPool: Pool[Kryo], inputPool: Pool[Input]): List[RawData] = {

    val kryo = kryoPool.obtain()
    val input = inputPool.obtain()
    val data = rowOfBinaryList.map(r => r.getAs[Array[Byte]]("message")).map{ binaryMsg =>
      /*val input = new Input(new ByteArrayInputStream(binaryMsg), 4096)
      val value = kryo.readClassAndObject(input).asInstanceOf[RawData]*/
      input.setInputStream(new ByteArrayInputStream(binaryMsg))
      val value = kryo.readClassAndObject(input).asInstanceOf[RawData]
      input.close()
      value
    }
    kryoPool.free(kryo)
    inputPool.free(input)
    data
  }

}
