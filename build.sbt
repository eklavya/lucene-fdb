name := "lucene-fdb"

version := "0.1"

scalaVersion := "2.12.6"

organization := "com.github.eklavya"

libraryDependencies ++= Seq(
  "org.apache.lucene" % "lucene-core" % "7.3.0",
  "org.apache.lucene" % "lucene-analyzers-common" % "7.3.0",
  "org.apache.lucene" % "lucene-queryparser" % "7.3.0"
)