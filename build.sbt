name := "SSKT"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "com.typesafe" % "config" % "1.3.1"

libraryDependencies += "org.apache.kafka" % "kafka_2.10" % "0.8.2.1"

libraryDependencies += "org.twitter4j" % "twitter4j-core" % "3.0.3"

libraryDependencies += "org.twitter4j" % "twitter4j-stream" % "3.0.3"

libraryDependencies += "com.twitter" % "bijection-avro_2.10" % "0.7.0"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "2.1.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "2.1.1"

libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-8_2.10" % "2.1.0"
    