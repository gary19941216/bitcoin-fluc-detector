name := "bit_fluc"
version := "1.0"
scalaVersion := "2.11.12"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.3", 
  "org.apache.spark" %% "spark-sql" % "2.4.3",
  "org.apache.hadoop" % "hadoop-aws" % "2.7.3",
  "com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.5.0",
  "com.johnsnowlabs.nlp" %% "spark-nlp" % "2.5.1",
  "com.twosigma" % "flint" % "0.6.0",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.3",
  "org.apache.kafka" % "kafka-clients" % "2.5.0",
)

