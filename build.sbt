name := "kafka-streams-sample"

version := "1.0"

scalaVersion := "2.12.1"
// https://mvnrepository.com/artifact/org.apache.kafka/kafka-streams
libraryDependencies ++= Seq(
  "joda-time" % "joda-time" % "2.9.2",

  //kafka-streams
  "org.apache.kafka" % "kafka-streams" % "0.11.0.0" exclude("com.fasterxml.jackson.core", "jackson-databind"),

  //akka
  "com.typesafe.akka" %% "akka-actor" % "2.5.2",

  //mocked-streams
  "com.madewithtea" %% "mockedstreams" % "1.3.0" % "test",

  //scalatest
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

        