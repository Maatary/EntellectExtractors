package entellect.extractors.mappers



import edu.isi.karma.rdf.GenericRDFGenerator
import edu.isi.karma.rdf.GenericRDFGenerator.InputType
import entellect.extractors._
import entellect.extractors.mappers.decoder._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import entellect.extractors.mappers.transformations._
import com.elsevier.entellect.commons._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object NormalizedDataMapper extends App {

  val spark        = SparkSession.builder()
    //.appName("SourceConvertor")
    //.master("local[*]")
    //.config("spark.executor.memory", "8G")
    .getOrCreate()

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("subscribe", "test")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 10000)
    .option("failOnDataLoss", false)
    .load().selectExpr("value as message")

  val rdf = df.mapPartitions{ binaryDfIt => {

    if (binaryDfIt.hasNext) {

      import KryoContext._
      import KarmaContext._
      import DataDecoderService._
      import DataTransformationService._
      import ExecutionContext._

      val rawDataList = decodeData(binaryDfIt.toList, kryoPool, inputPool)
      val groups      = rawDataList.groupBy(rd => (rd.modelName, rd.sourceType)).toSeq
      val seqFuture   = groups.foldLeft { Seq(Future.successful(Seq[Row]())) } {(a, b) =>
        a ++ mapRawNormalizedDataToRowRdf(rdfGenerator, b,  InputType.OBJECT, context)
      }
      val futureSeq   = Future.sequence(seqFuture)
      val rdfRows     = Await.result(futureSeq, Duration.Inf).flatten

      rdfRows.toIterator

    }else
      Iterator[Row]()
  }
  }(RowEncoder.apply(StructType(List(StructField("value", StringType, false)))))


  rdf
    .writeStream
    .trigger(Trigger.ProcessingTime("1 seconds"))
    .format("kafka")
    .outputMode("append")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("topic", "NormalizedSourceRDF")
    .option("checkpointLocation","sparkoutputs/checkpoints")
    .queryName("NormalizedDataMapping").start().awaitTermination()
}