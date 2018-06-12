name := "calldb"

version := "0.0.14"

scalaVersion := "2.11.8"

organization := "com.github.mirabout.calldb"

resolvers += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"

// TODO: eliminate postgresql-jdbc dependency (it is used for HStore parser only)
libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % "2.11.8",
  "com.github.mauricio" %% "postgresql-async" % "0.2.15",
  "org.postgresql" % "postgresql" % "9.3-1103-jdbc41",
  "com.google.inject" % "guice" % "4.0",
  "org.specs2" %% "specs2-core" % "3.0" % "test",
  "com.google.guava" % "guava" % "19.0" % "test")

scalacOptions in Test ++= Seq("-Yrangepos")

fork in Test := false

parallelExecution in Test := false

testForkedParallel in Test := false

concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)

testFrameworks := Seq(TestFrameworks.Specs2)

exportJars := true
