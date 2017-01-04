name := "dogberry"

version := "1.0"

scalaVersion := "2.12.0"

libraryDependencies += "junit" % "junit" % "4.10" % "test"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

// https://mvnrepository.com/artifact/com.datastax.cassandra/cassandra-driver-core
libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "3.1.2"
    