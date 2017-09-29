name := "engage_data"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "2.0.0" % "provided",
"org.apache.spark" %% "spark-sql" % "2.0.0" % "provided",
"org.apache.spark" %% "spark-streaming" % "2.0.0" % "provided",
"org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.0.0",
"net.debasishg" %% "redisclient" % "3.3",
"edu.stanford.nlp" % "stanford-corenlp" % "3.6.0" ,
"edu.stanford.nlp" % "stanford-corenlp" % "3.6.0" % "test" classifier "models",
"databricks" % "spark-corenlp" % "0.2.0-s_2.11"
)



mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}

resolvers += "SparkPackages" at "https://dl.bintray.com/spark-packages/maven/"
resolvers += Resolver.url("bintray-sbt-plugins", url("http://dl.bintray.com/sbt/sbt-plugin-releases"))(Resolver.ivyStylePatterns)
