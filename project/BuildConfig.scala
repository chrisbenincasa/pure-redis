import sbt._
import sbt.Keys._

object BuildConfig {
  object Dependencies {
    val testDeps = Seq(
      "org.scalatest" %% "scalatest" % versions.scalatest,
      "org.scalacheck" %% "scalacheck" % "1.13.4"
    ).map(_ % "test,it")
  }

  object Revision {
    lazy val revision = System.getProperty("revision", "SNAPSHOT")
  }

  object versions {
    val scalatest = "3.0.1"
  }

  def commonSettings(currentVersion: String) = {
    Seq(
      organization := "com.chrisbenincasa",

      version := s"${currentVersion}-${BuildConfig.Revision.revision}",

      credentials += Credentials(Path.userHome / ".sbt" / "credentials"),

      scalaVersion := "2.12.7",

      crossScalaVersions := Seq("2.11.11", scalaVersion.value),

      scalacOptions ++= Seq(
        "-deprecation",
        "-encoding", "UTF-8",
        "-feature",
        "-language:existentials",
        "-language:higherKinds",
        "-language:implicitConversions",
        "-language:postfixOps",
        "-language:experimental.macros",
        "-unchecked",
        "-Ywarn-nullary-unit",
        "-Xfatal-warnings",
        "-Ywarn-dead-code",
        "-Xfuture"
      ) ++ (CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, 12)) => Seq("-Xlint:-unused")
        case _ => Seq("-Xlint")
      }),

      scalacOptions in doc := scalacOptions.value.filterNot(_ == "-Xfatal-warnings"),   

      publishMavenStyle := true
    )
  }
}
