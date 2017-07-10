name := "kafka-streams-sample"

version := "1.0"

scalaVersion := "2.12.1"
// https://mvnrepository.com/artifact/org.apache.kafka/kafka-streams
libraryDependencies ++= Seq(
  "joda-time" % "joda-time" % "2.9.2",
  "org.apache.kafka" % "kafka-streams" % "0.11.0.0",
  "com.typesafe.akka" %% "akka-actor" % "2.5.2"
)

        