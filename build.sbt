name := """OuiOuistiti"""

version := "1.0"

scalaVersion := "2.11.7"


resolvers +=
  "Clojars repository" at "http://clojars.org/repo"

// Change this to another test framework if you prefer
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.3.12"
libraryDependencies += "org.twitter4j" % "twitter4j-core" % "4.0.4"
libraryDependencies += "org.mongodb" % "casbah-core_2.11" % "2.8.2"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.6.4"

libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.0.0"

libraryDependencies += "com.norconex.language" % "langdetect" % "1.3.0"
libraryDependencies +=  "org.scalaj" %% "scalaj-http" % "1.1.5"

libraryDependencies += "org.clojars.smallrivers" % "snacktory" % "1.2-SNAPSHOT"
libraryDependencies += "net.ruippeixotog" %% "scala-scraper" % "0.1.1"

libraryDependencies += "edu.stanford.nlp" % "stanford-parser" % "3.5.2"

Revolver.settings
