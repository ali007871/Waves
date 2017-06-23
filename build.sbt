import com.typesafe.config.ConfigFactory
import sbt.Keys._
import sbtassembly.MergeStrategy

val appConf = ConfigFactory.parseFile(new File("src/main/resources/reference.conf")).resolve().getConfig("app")

inThisBuild(Seq(
  organization in ThisBuild := "com.wavesplatform",
  name := "waves",
  version := appConf.getString("version"),
  scalaVersion := "2.12.1"
))

scalacOptions ++= Seq("-feature", "-deprecation", "-Xmax-classfile-name", "128")

resolvers ++= Seq(
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "SonaType" at "https://oss.sonatype.org/content/groups/public",
  "Typesafe maven releases" at "http://repo.typesafe.com/typesafe/maven-releases/")

val kamonVersion = "0.6.7"

libraryDependencies ++=
  Dependencies.db ++
  Dependencies.http ++
  Dependencies.akka ++
  Dependencies.serialization ++
  Dependencies.testKit ++
  Dependencies.logging ++
  Dependencies.matcher ++
  Dependencies.p2p ++
  Seq(
    "com.iheart" %% "ficus" % "1.4.0",
    "org.scorexfoundation" %% "scrypto" % "1.2.0",
    "commons-net" % "commons-net" % "3.+",
    "com.github.pathikrit" %% "better-files" % "2.17.+",
    "org.typelevel" %% "cats-core" % "0.9.0",
    "io.kamon" %% "kamon-core" % kamonVersion,
    "io.kamon" %% "kamon-akka-2.4" % kamonVersion,
    "io.kamon" %% "kamon-statsd" % kamonVersion,
    "io.kamon" %% "kamon-log-reporter" % kamonVersion,
    "io.kamon" %% "kamon-system-metrics" % kamonVersion
  )

inConfig(Test)(Seq(
  javaOptions += "-Dlogback.configurationFile=logback-sbt.xml",
  fork := true,
  parallelExecution := false,
  testOptions += Tests.Argument("-oIDOF", "-u", "target/test-reports")
))

concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)

import com.typesafe.sbt.SbtAspectj._
aspectjSettings

javaOptions <++= AspectjKeys.weaverOptions in Aspectj

// Create a new MergeStrategy for aop.xml files
val aopMerge: MergeStrategy = new MergeStrategy {
  val name = "aopMerge"
  import scala.xml._
  import scala.xml.dtd._

  def apply(tempDir: File, path: String, files: Seq[File]): Either[String, Seq[(File, String)]] = {
    val dt = DocType("aspectj", PublicID("-//AspectJ//DTD//EN", "http://www.eclipse.org/aspectj/dtd/aspectj.dtd"), Nil)
    val file = MergeStrategy.createMergeTarget(tempDir, path)
    val xmls: Seq[Elem] = files.map(XML.loadFile)
    val aspectsChildren: Seq[Node] = xmls.flatMap(_ \\ "aspectj" \ "aspects" \ "_")
    val weaverChildren: Seq[Node] = xmls.flatMap(_ \\ "aspectj" \ "weaver" \ "_")
    val options: String = xmls.map(x => (x \\ "aspectj" \ "weaver" \ "@options").text).mkString(" ").trim
    val weaverAttr = if (options.isEmpty) Null else new UnprefixedAttribute("options", options, Null)
    val aspects = new Elem(null, "aspects", Null, TopScope, false, aspectsChildren: _*)
    val weaver = new Elem(null, "weaver", weaverAttr, TopScope, false, weaverChildren: _*)
    val aspectj = new Elem(null, "aspectj", Null, TopScope, false, aspects, weaver)
    XML.save(file.toString, aspectj, "UTF-8", xmlDecl = false, dt)
    IO.append(file, IO.Newline.getBytes(IO.defaultCharset))
    Right(Seq(file -> path))
  }
}

assemblyMergeStrategy in assembly := {
  case "META-INF/aop.xml" => aopMerge
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

test in assembly := {}
