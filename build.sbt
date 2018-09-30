
lazy val commonSettings = Seq(
  organization  := "com.elsevier.entellect",
  version       := "0.1.0-SNAPSHOT",
  scalaVersion  := "2.11.12",
  resolvers     += Resolver.mavenLocal,
  updateOptions := updateOptions.value.withLatestSnapshots(false)
)

lazy val entellectextractors = (project in file("."))
  .settings(commonSettings).aggregate(entellectextractorscommon, entellectextractorsfetchers, entellectextractorsmappers, entellectextractorsconsumers)

lazy val entellectextractorscommon = project
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(
      "com.esotericsoftware" % "kryo" % "5.0.0-RC1",
      "com.github.romix.akka" %% "akka-kryo-serialization" % "0.5.0" excludeAll(excludeJpountz),
      "org.apache.kafka" % "kafka-clients" % "1.0.1",
      "com.typesafe.akka" %% "akka-stream" % "2.5.16",
      "com.typesafe.akka" %% "akka-http-spray-json" % "10.1.4",
      "com.typesafe.akka" % "akka-slf4j_2.11" % "2.5.16",
      "ch.qos.logback" % "logback-classic" % "1.2.3"
    )
  )

lazy val entellectextractorsfetchers = project
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream-kafka" % "0.22",
      "com.typesafe.slick" %% "slick" % "3.2.3",
      "com.typesafe.slick" %% "slick-hikaricp" % "3.2.3",
      "com.lightbend.akka" %% "akka-stream-alpakka-slick" % "0.20") 
  )
  .dependsOn(entellectextractorscommon)

lazy val entellectextractorsconsumers = project
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream-kafka" % "0.22")
  )
  .dependsOn(entellectextractorscommon)

lazy val entellectextractorsmappers = project
  .settings(
      commonSettings,
      mainClass in assembly := Some("entellect.extractors.mappers.NormalizedDataMapper"),
      assemblyMergeStrategy in assembly := {
        case "application.conf" => MergeStrategy.concat
        case "reference.conf"   => MergeStrategy.concat
        case PathList("META-INF", "services", "org.apache.jena.system.JenaSubsystemLifecycle") => MergeStrategy.concat
        case PathList("META-INF", "services", "org.apache.spark.sql.sources.DataSourceRegister") => MergeStrategy.concat
        case PathList("META-INF", xs @ _*) => MergeStrategy.discard
        case x => MergeStrategy.first},
      dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.5",
      dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.5",
      dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.5",
      dependencyOverrides += "org.apache.jena" % "apache-jena" % "3.8.0",
      libraryDependencies ++= Seq(
      "org.apache.jena" % "apache-jena" % "3.8.0",
      "edu.isi" % "karma-offline" % "0.0.1-SNAPSHOT",
      "org.apache.spark" % "spark-core_2.11" % "2.3.1" % "provided",
      "org.apache.spark" % "spark-sql_2.11" % "2.3.1" % "provided",
      "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.3.1"
      //"com.datastax.cassandra" % "cassandra-driver-core" % "3.5.1"
    ))
  .dependsOn(entellectextractorscommon)



lazy val excludeJpountz = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")

//lazy val kafkaClients = "org.apache.kafka" % "kafka-clients" % "1.0.1" excludeAll(excludeJpountz)

