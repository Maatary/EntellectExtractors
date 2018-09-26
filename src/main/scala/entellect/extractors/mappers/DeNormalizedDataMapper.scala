package entellect.extractors.mappers

import edu.isi.karma.rdf.GenericRDFGenerator.InputType
import entellect.extractors._
import entellect.extractors.mappers.decoder._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import entellect.extractors.mappers.transformations._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

object DeNormalizedDataMapper extends App {

  val spark        = SparkSession.builder()
    .appName("SourceConvertor")
    .master("local[*]")
    .config("spark.executor.memory", "12G")
    .getOrCreate()

  spark.conf.set("spark.sql.shuffle.partitions", 8)

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("subscribe", "DeNormalizedRawData")
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
        a ++ mapRawDeNormalizedDataToRowRdf(rdfGenerator, b,  InputType.JL, context)
      }
      val futureSeq   = Future.sequence(seqFuture)
      val rdfRows     = Await.result(futureSeq, Duration.Inf).flatten

      rdfRows.toIterator

    }else
      Iterator[Row]()
  }
  }(RowEncoder.apply(StructType(List(
        StructField("key", StringType, false),
        StructField("value", StringType, false)
      )))
  )


  rdf
    .dropDuplicates("key")
    //.repartition(8)
    .drop("key")
    .writeStream
    .trigger(Trigger.ProcessingTime("30 seconds"))
    .format("kafka")
    .outputMode("append")
    .option("kafka.bootstrap.servers", "127.0.0.1:9092")
    .option("topic", "NormalizedSourceRDF")
    .option("checkpointLocation","sparkoutputs/checkpoints")
    .queryName("DeNormalizedDataMapping").start().awaitTermination()

}