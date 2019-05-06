name := "jaeger-kafka-haystack-collector"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.0.5",
  "org.apache.kafka" % "kafka-clients" % "2.1.1",
  "org.apache.kafka" %% "kafka-streams-scala" % "2.1.1",
  "org.json4s" %% "json4s-ext" % "3.6.5",
  "org.json4s" %% "json4s-ast" % "3.6.5",
  "org.json4s" %% "json4s-native" % "3.6.5", 
  "com.thesamet.scalapb" %% "scalapb-json4s" % "0.7.0",
  "org.slf4j" % "slf4j-api" % "1.7.5",
  "org.slf4j" % "slf4j-simple" % "1.7.5"
)

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)

enablePlugins(JavaAppPackaging)
enablePlugins(DockerPlugin)

dockerBaseImage := "openjdk:jre"
