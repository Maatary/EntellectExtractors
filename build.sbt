name         := "EntellectExtractors"
version      := "0.1"
scalaVersion := "2.11.12"


resolvers           += Resolver.mavenLocal
updateOptions       := updateOptions.value.withLatestSnapshots(false)

libraryDependencies ++= Seq(
  "edu.isi" % "karma-offline" % "0.0.1-SNAPSHOT",
  "org.apache.spark" % "spark-core_2.11" % "2.3.1",
  "org.apache.kafka" % "kafka-clients" % "1.0.1",
  "org.apache.spark" % "spark-sql_2.11" % "2.3.1",
  "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.3.1",
  "com.typesafe.akka" %% "akka-stream" % "2.5.16",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.1.4",
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.22",
  "com.github.romix.akka" %% "akka-kryo-serialization" % "0.5.0" excludeAll(excludeJpountz),
  "com.esotericsoftware" % "kryo" % "5.0.0-RC1",
  "com.typesafe.slick" %% "slick" % "3.2.3",
  //"org.slf4j" % "slf4j-nop" % "1.6.4",
  "com.typesafe.slick" %% "slick-hikaricp" % "3.2.3",
  "com.lightbend.akka" %% "akka-stream-alpakka-slick" % "0.20",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.5.1"
)

lazy val excludeJpountz = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")

lazy val kafkaClients = "org.apache.kafka" % "kafka-clients" % "1.0.1" excludeAll(excludeJpountz)