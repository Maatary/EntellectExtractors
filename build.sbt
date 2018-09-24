name         := "EntellectExtractors"
version      := "0.1"
scalaVersion := "2.11.12"


resolvers           += Resolver.mavenLocal
updateOptions       := updateOptions.value.withLatestSnapshots(false)

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.5"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.5"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.5"
dependencyOverrides += "org.apache.jena" % "apache-jena" % "3.8.0"

libraryDependencies ++= Seq(
  "org.apache.jena" % "apache-jena" % "3.8.0",
  "edu.isi" % "karma-offline" % "0.0.1-SNAPSHOT",
  "org.apache.spark" % "spark-core_2.11" % "2.3.1",
  "org.apache.kafka" % "kafka-clients" % "1.0.1",
  "org.apache.spark" % "spark-sql_2.11" % "2.3.1",
  "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.3.1",
  "com.typesafe.akka" %% "akka-stream" % "2.5.16",
  "com.typesafe.akka" %% "akka-http-spray-json" % "10.1.4",
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.22",
  "com.typesafe.akka" % "akka-slf4j_2.11" % "2.5.16",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.github.romix.akka" %% "akka-kryo-serialization" % "0.5.0" excludeAll(excludeJpountz),
  "com.esotericsoftware" % "kryo" % "5.0.0-RC1",
  "com.typesafe.slick" %% "slick" % "3.2.3",
  "com.typesafe.slick" %% "slick-hikaricp" % "3.2.3",
  "com.lightbend.akka" %% "akka-stream-alpakka-slick" % "0.20"
  //"com.datastax.cassandra" % "cassandra-driver-core" % "3.5.1"
)

lazy val excludeJpountz = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")

lazy val kafkaClients = "org.apache.kafka" % "kafka-clients" % "1.0.1" excludeAll(excludeJpountz)