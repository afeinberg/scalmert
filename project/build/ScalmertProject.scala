import sbt._

class ScalmertProject(info: ProjectInfo) 
extends DefaultProject(info) with IdeaProject {
  val localRepo = "Local Maven Repository" at "file:///Users/alex/.m2/repository" 
  val scalaToolsRepo = "Scala-Tools Maven Repository" at
  "http://nexus.scala-tools.org/content/repositories/snapshots/"
  
  val specs = "org.scala-tools.testing" %% "specs" % "1.6.5" % "test"

  val scalajCollections = "org.scalaj" %% "scalaj-collection" % "1.0" 
  val googleCollections = "com.google.collections" % "google-collections" % "1.0"
  val commonsLang = "commons-lang" % "commons-lang" % "2.4"
  val jdom = "jdom" % "jdom" % "1.1"
  val log4j = "log4j" % "log4j" % "1.2.14"

  val voldemort = "voldemort" % "voldemort" % "0.81" withSources()
}
