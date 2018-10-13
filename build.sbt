import Dependencies._
import Shared._
import sbt.Keys.libraryDependencies
import sbtbuildinfo.Plugin._

organization := MainProperties.organization

name := MainProperties.name

scalaVersion := defaultScalaVersion

val SparkNotebookSimpleVersion = "0.9.0-SNAPSHOT"

version in ThisBuild := SparkNotebookSimpleVersion

import play.sbt.PlayImport._
play.sbt.PlayImport.PlayKeys.playDefaultPort := 9001

updateOptions := updateOptions.value.withCachedResolution(true)

maintainer := DockerProperties.maintainer

enablePlugins(UniversalPlugin)

enablePlugins(DockerPlugin)

enablePlugins(GitVersioning)

enablePlugins(GitBranchPrompt)

import uk.gov.hmrc.gitstamp.GitStampPlugin._

import com.typesafe.sbt.SbtNativePackager.autoImport.NativePackagerHelper._

dockerBaseImage := DockerProperties.baseImage

dockerCommands ++= DockerProperties.commands

dockerExposedVolumes ++= DockerProperties.volumes

dockerExposedPorts ++= DockerProperties.ports

dockerRepository := DockerProperties.registry //Docker

packageName in Docker := MainProperties.name


// DEBIAN PACKAGE
enablePlugins(DebianPlugin)

name in Debian := MainProperties.name

maintainer in Debian := DebianProperties.maintainer

packageDescription := "Interactive and Reactive Data Science using Scala and Spark."

debianPackageDependencies in Debian += "java7-runtime | java7-runtime-headless"

serverLoading in Debian := DebianProperties.serverLoading

daemonUser in Linux := DebianProperties.daemonUser

daemonGroup in Linux := DebianProperties.daemonGroup

version := sys.props.get("deb-version").getOrElse(version.value)

import DebianConstants._
maintainerScripts in Debian := maintainerScriptsAppend((maintainerScripts in Debian).value)(
  Postinst -> (
    s"chown -R ${DebianProperties.daemonUser}:${DebianProperties.daemonGroup} " +
      s"/usr/share/${MainProperties.name}/"
  )
)

ivyScala := ivyScala.value map {
  _.copy(overrideScalaVersion = true)
}

parallelExecution in Test in ThisBuild := false

// these java options are for the forked test JVMs
javaOptions in ThisBuild ++= Seq("-Xmx512M")

val viewerMode = Option(sys.env.getOrElse("VIEWER_MODE", "false")).get.toBoolean

val buildInfoValues = Seq[BuildInfoKey](
  "sparkNotebookVersion" → SparkNotebookSimpleVersion,
  scalaVersion,
  sparkVersion,
  hadoopVersion,
  withHive,
  jets3tVersion,
  jlineDef,
  sbtVersion,
  git.formattedShaVersion,
  BuildInfoKey.action("buildTime") {
    val formatter = new java.text.SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy")
    formatter.format(new java.util.Date(System.currentTimeMillis))
  },
  "viewer" → viewerMode
)


/*
  adding nightly build resolver like
  https://repository.apache.org/content/repositories/orgapachespark-1153
  this props will only need the number 1153 at the end
*/
val searchSparkResolver = Option(sys.props.getOrElse("spark.resolver.search", "false")).get.toBoolean
val sparkResolver = Option(sys.props.getOrElse("spark.resolver.id", null))

resolvers in ThisBuild ++= Seq(
  // P.S. coursier was not working well with mavenLocal (FIXED recently in 1.0.0-RC4) https://goo.gl/FeKuEo
  Resolver.mavenLocal,
  Resolver.typesafeRepo("releases"),
  Resolver.sonatypeRepo("releases"),
  Resolver.typesafeIvyRepo("releases"),
  Resolver.typesafeIvyRepo("snapshots"),
  "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos",
  "mapr" at "http://repository.mapr.com/maven",
  // docker
  "softprops-maven" at "http://dl.bintray.com/content/softprops/maven"
) ++ ((sparkResolver, searchSparkResolver, sparkVersion.value) match {
  case (Some(x), _, _) => Seq(
                    ("spark " + x) at ("https://repository.apache.org/content/repositories/orgapachespark-"+x)
                  )
  case (None, true, sv)  =>
    println(s"""|
    |**************************************************************
    | SEARCHING for Spark Nightly repo for version ~ $sv
    |**************************************************************
    |""".stripMargin)
    import scala.io.Source.fromURL
    val mainPage = "https://repository.apache.org/content/repositories/"
    val repos = fromURL(mainPage).mkString
    val c = repos.split("\n").toList.filter(!_.contains("<link")).mkString
    val r = scala.xml.parsing.XhtmlParser(scala.io.Source.fromString(c))
    val as = r \\ "a"
    val sparks:List[Option[String]] = as
      .filter(_.toString.contains("orgapachespark"))
      .map(_.attribute("href").map(_.head.text))
      .toList
    val sparkRepos = sparks.collect { case Some(l) =>
      val id = l.reverse.tail.takeWhile(_.isDigit).reverse.toInt
      val u = l + "org/apache/spark/spark-core_2.10/"
      val repo = fromURL(u).mkString
      val c = repo.split("\n").toList.filter(!_.contains("<link")).mkString
      val r = scala.xml.parsing.XhtmlParser(scala.io.Source.fromString(c))
      val v = (r \\ "table" \\ "a").map(_.text).filter(_ != "Parent Directory").head.replace("/", "")
      (l, id, v)
    }
    .groupBy(_._3).mapValues(_.maxBy(_._2)._1)
    .map{ case (v, url) => (v == sv, v, url) }
    println("======================================================================== ")
    println("Found these repos")
    println(sparkRepos.map{case (cc, v, url) => s"${if(cc) "[x]" else "[ ]" } $v: $url"}.mkString("\n"))

    sparkRepos.filter(_._1).map{ case (_, v, url) => ("spark " + v) at url }

  case (None, false, _) => Nil
})

compileOrder := CompileOrder.Mixed

publishMavenStyle := false

publishArtifact in Test := false

javacOptions ++= Seq("-Xlint:deprecation", "-g")

scalacOptions ++= Seq("-deprecation", "-feature")

scalacOptions ++= Seq("-Xmax-classfile-name", "100")

scriptClasspath in batScriptReplacements := Seq("*")

batScriptExtraDefines += {
  "set \"APP_CLASSPATH=%CLASSPATH_OVERRIDES%;%YARN_CONF_DIR%;%HADOOP_CONF_DIR%;%EXTRA_CLASSPATH%;%APP_LIB_DIR%\\..\\conf;%APP_CLASSPATH%\""
}
batScriptExtraDefines += {
  "set \"VIEWER_MODE="+viewerMode+"\""
}

val ClasspathPattern = "declare -r app_classpath=\"(.*)\"\n".r

bashScriptDefines := bashScriptDefines.value.map {
  case ClasspathPattern(classpath) =>
    s"""declare -r app_classpath="$${CLASSPATH_OVERRIDES}:$${YARN_CONF_DIR}:$${HADOOP_CONF_DIR}:$${EXTRA_CLASSPATH}:$classpath"\n""" +
    s"""export VIEWER_MODE=$viewerMode"""
  case entry => entry
}

dependencyOverrides ++= log4j2.toSet

dependencyOverrides += guava

sharedSettings

//libraryDependencies ++= playDeps

resolvers ++= Seq("Sonatype snapshots repository" at "https://oss.sonatype.org/content/repositories/snapshots/")
libraryDependencies ++= pac4jSecurity

libraryDependencies += scalaTest
libraryDependencies += "com.google.inject" % "guice" % "4.0"

routesGenerator := StaticRoutesGenerator

libraryDependencies ++= List(
  akka,
  akkaRemote,
  akkaSlf4j,
  cache,
  commonsIO,
  commonsExec,
  commonsCodec,
  "org.scala-lang" % "scala-library" % defaultScalaVersion,
  "org.scala-lang" % "scala-reflect" % defaultScalaVersion,
  "org.scala-lang" % "scala-compiler" % defaultScalaVersion
)

//for aether and compensating for 2.11 modularization
libraryDependencies ++= {
  scalaBinaryVersion.value match {
    case "2.10" => Nil
    case "2.11" => List( 
      "org.scala-lang.modules" %% "scala-xml" % "1.0.4",
      "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"
    )
  }
}
libraryDependencies ~= (_.map(excludeSpecs2))

lazy val sbtDependencyManager = Project(id = "sbt-dependency-manager", base = file("modules/sbt-dependency-manager"))
  .settings(
    version := version.value,
    libraryDependencies += scalaTest
  )
  .settings(sharedSettings: _*)
  .settings(
    Extra.sbtDependencyManagerSettings
  )

lazy val sparkNotebookCore = Project(id = "spark-notebook-core", base = file("modules/core"))
  .settings(
    version := version.value,
    libraryDependencies ++= playJson,
    libraryDependencies ++= log4j2,
    libraryDependencies += commonsIO,
    libraryDependencies += scalaTest
  ).settings(sharedSettings: _*)
  .settings(
    Extra.sparkNotebookCoreSettings
  )

lazy val gitNotebookProvider = Project(id = "git-notebook-provider", base = file("modules/git-notebook-provider"))
  .settings(
    version := version.value
  )
  .settings(sharedSettings: _*)
  .dependsOn(sparkNotebookCore)
  .settings(
    Extra.gitNotebookProviderSettings
  )

lazy val sbtProjectGenerator = Project(id = "sbt-project-generator", base = file("modules/sbt-project-generator"))
  .settings(
    version := version.value
  )
.settings(sharedSettings: _*)
.settings(
  libraryDependencies += commonsIO,
  libraryDependencies += scalaTest
).dependsOn(
  sbtDependencyManager,
  sparkNotebookCore,
  gitNotebookProvider
)
  .settings(buildInfoSettings: _*)
  .settings(
    sourceGenerators in Compile += buildInfo,
    buildInfoKeys :=  buildInfoValues,
    buildInfoPackage := "notebook"
  )

val fullVersion = Def.setting { 
    Seq(
      s"${version.in(ThisBuild).value}",
      s"-scala-${scalaVersion.value}",
      s"-spark-${sparkVersion.value}",
      s"-hadoop-${hadoopVersion.value}",
      if(withHive.value) "-with-hive" else ""
    ).mkString
  }

val playRemoveNetty = {
  val result = sparkVersionTuple match {
    case _ if Ordering.apply[(Int, Int, Int)].lt(sparkVersionTuple, (2, 3, 0)) => false
    case _ => true
  }
  println(s"playRemoveNetty: $result")
  result
}

// in spark 2.3.0 dont use PlayNetty to prevent a Netty conflict
def rootProject = {
  val root = project.in(file("."))
  if (playRemoveNetty)
    root.enablePlugins(PlayScala, PlayAkkaHttpServer).disablePlugins(PlayNettyServer)
  else root.enablePlugins(PlayScala)
}

lazy val sparkNotebook = rootProject
  // https://www.playframework.com/documentation/2.5.x/SettingsLogger#Using-a-Custom-Logging-Framework
  .disablePlugins(PlayLogback)
  .aggregate(sparkNotebookCore, gitNotebookProvider, sbtDependencyManager, sbtProjectGenerator, subprocess, observable, common, spark, kernel, collector)
  .dependsOn(sparkNotebookCore, gitNotebookProvider, subprocess, observable, sbtProjectGenerator, common, spark, kernel, collector)
  .settings(sharedSettings: _*)
  .settings(
    bashScriptExtraDefines += s"""export ADD_JARS="$${ADD_JARS},$${lib_dir}/$$(ls $${lib_dir} | grep ${organization.value}.common | head)"""",
    mappings in Universal ++= directory("notebooks"),
    version in Universal := fullVersion.value,
    version in Docker := fullVersion.value,
    version in Debian := fullVersion.value,
    mappings in Docker ++= directory("notebooks")
  )
  .settings(includeFilter in(Assets, LessKeys.less) := "*.less")
  .settings(unmanagedSourceDirectories in Compile := Seq(scalaSource.in(Compile).value)) //avoid app-2.10 and co to be created
  .settings(
    git.useGitDescribe := true,
    git.baseVersion := SparkNotebookSimpleVersion
  )
  .settings(
    gitStampSettings: _*
  )
  .settings(
    Extra.sparkNotebookSettings
  )
  .settings(libraryDependencies ~= (_.map(excludeSpecs2)))

lazy val subprocess = Project(id="subprocess", base=file("modules/subprocess"))
  .settings(libraryDependencies ++= playDeps)
  .settings(
    libraryDependencies ++= {
      Seq(
        akka,
        akkaRemote,
        akkaSlf4j,
        commonsIO,
        commonsExec,

        hadoopClient(defaultHadoopVersion)
      ) ++ log4j2
    }
  )
  .settings(sharedSettings: _*)
  .settings(
    Extra.subprocessSettings
  )
  .settings(libraryDependencies ~= (_.map(excludeSpecs2)))


lazy val observable = Project(id = "observable", base = file("modules/observable"))
  .dependsOn(subprocess, sparkNotebookCore)
  .settings(
    libraryDependencies ++= Seq(
      akkaRemote,
      akkaSlf4j,
      rxScala
    ) ++ log4j2
  )
  .settings(sharedSettings: _*)
  .settings(
    Extra.observableSettings
  )

val versionShortWithSpark = Def.setting {
  s"${sparkVersion.value}_${version.in(ThisBuild).value}"
}

val commonProjSparkDir = sparkVersionTuple match {
  case _ if Ordering.apply[(Int, Int, Int)].lt(sparkVersionTuple, (2, 3, 0)) =>
    "spark-pre-2.3"
  case _ => "spark-post-2.3"
}

lazy val common = Project(id = "common", base = file("modules/common"))
  .dependsOn(observable, sbtDependencyManager)
  .settings(
    version := versionShortWithSpark.value
  )
  .settings(
    libraryDependencies ++= Seq(
      akka

    ) ++ log4j2,
    libraryDependencies += scalaTest,
    unmanagedSourceDirectories in Compile += (sourceDirectory in Compile).value / ("scala-" + scalaBinaryVersion.value),
    unmanagedSourceDirectories in Compile += (sourceDirectory in Compile).value / commonProjSparkDir
  )
  .settings(
    gisSettings
  )
  .settings(sharedSettings: _*)
  .settings(sparkSettings: _*)
  .settings(buildInfoSettings: _*)
  .settings(
    sourceGenerators in Compile += buildInfo,
    buildInfoKeys := buildInfoValues,
    buildInfoPackage := "notebook"
  )
  .settings(
    Extra.commonSettings
  )

lazy val spark = Project(id = "spark", base = file("modules/spark"))
  .dependsOn(common, subprocess, observable)
  .settings(
    version := versionShortWithSpark.value
  )
  .settings(
    libraryDependencies ++= Seq(
      akkaRemote,
      akkaSlf4j,

      commonsIO
    ) ++ log4j2, 
    libraryDependencies ++= Seq(
      jlineDef.value._1 % "jline" % jlineDef.value._2,
      "org.scala-lang" % "scala-compiler" % scalaVersion.value
    ),
    unmanagedSourceDirectories in Compile += {
      def folder(v:String, sv:String) = {
          val tsv = sv match { case extractVs(major,minor,patch) => (major.toInt,minor.toInt,patch.toInt) }

          val scalaVerDir = (sourceDirectory in Compile).value / ("scala_" + v)

          tsv match {
            // THIS IS HOW WE CAN DEAL WITH RADICAL CHANGES
            //case _ if Ordering.apply[(Int, Int, Int)].lt(tsv, LAST_WORKING_VERSION) => scalaVerDir / "spark-pre"+LAST_WORKING_VERSION
            case _ => scalaVerDir / "spark-last"
          }
      }
      (scalaBinaryVersion.value, sparkVersion.value.takeWhile(_ != '-')) match {
        case (v, sv) if v startsWith "2.10" => folder("2.10", sv)
        case (v, sv) if v startsWith "2.11" => folder("2.11", sv)
        case (v, _) => throw new IllegalArgumentException("Bad scala version: " + v)
      }
    }
  )
  .settings(sharedSettings: _*)
  .settings(sparkSettings: _*)
  .settings(
    Extra.sparkSettings
  )

lazy val kernel = Project(id = "kernel", base = file("modules/kernel"))
  .dependsOn(common, sbtDependencyManager, subprocess, observable, spark)
  .settings(
    libraryDependencies ++= Seq(
      akkaRemote,
      akkaSlf4j,
      commonsIO
    ) ++ log4j2,
    libraryDependencies += scalaTest,
    unmanagedSourceDirectories in Compile += (sourceDirectory in Compile).value / ("scala-" + scalaBinaryVersion.value)
  )
  .settings(sharedSettings: _*)
  .settings(
    Extra.kernelSettings
  )

lazy val collector = Project(id = "collector", base = file("modules/collector"))
  .dependsOn(common, subprocess, observable)
  .settings(libraryDependencies ++= collectorDeps)
  .settings(sharedSettings: _*)
  .settings(                      
    Extra.kernelSettings
  )

javaOptions += "-Dsbt.jse.engineType=Node"
javaOptions += " -Dsbt.jse.command=$(which node)"
excludeDependencies ++= Seq("org.slf4j" % "slf4j-log4j12", "org.sonatype.sisu" % "sisu-guava")
dependencyOverrides ++= Set("com.google.guava" % "guava" % "16.0.1")
