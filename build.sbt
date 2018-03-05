import sbt._
import BuildConfig.Dependencies

lazy val commonSettings = BuildConfig.commonSettings(currentVersion = "1.0")

commonSettings

name := "redis-fun"

libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-core" % "1.0.1",
  "org.typelevel" %% "cats-effect" % "0.9"
) ++ Dependencies.testDeps

lazy val showVersion = taskKey[Unit]("Show version")

showVersion := {
  println(version.value)
}

// custom alias to hook in any other custom commands
addCommandAlias("build", "; compile")
