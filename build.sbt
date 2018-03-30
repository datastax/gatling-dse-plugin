// WIP: get the credentials from other build systems to avoid yet another config file
//import scala.xml._
//val mavenSettings:Elem = XML.loadFile(new File(System.getProperty("user.home") + "/.m2/settings.xml"))
//val artifactoryServer = mavenSettings \ "servers" \ "server" filter { node => (node \ "id").text == "artifactory" }
//val artifactoryUsername = (artifactoryServer \ "username").text
//val artifactoryPassword = (artifactoryServer \ "password").text

// WIP: these URL are for DS repo, publishing is not yet added to the build
//datastaxPublishingRepositoryUrl=http://datastax.artifactoryonline.com/datastax/datastax-releases-local
//datastaxPublishingSnapshotRepositoryUrl=http://datastax.artifactoryonline.com/datastax/datastax-snapshots-local

scalacOptions += "-target:jvm-1.8"

libraryDependencies += "com.datastax.dse"             %  "dse-java-driver-core"     % "1.5.1"
libraryDependencies += "com.datastax.dse"             %  "dse-java-driver-graph"    % "1.5.1"
libraryDependencies += "com.github.nscala-time"       %% "nscala-time"              % "2.18.0"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala"     % "2.9.1"
libraryDependencies += "org.hdrhistogram"             %  "HdrHistogram"             % "2.1.10"

libraryDependencies += "io.gatling.highcharts"        % "gatling-charts-highcharts" % "2.3.0"   % Provided

libraryDependencies += "org.fusesource"               %  "sigar"                    % "1.6.4"   % Test
libraryDependencies += "org.scalatest"                %% "scalatest"                % "3.0.5"   % Test
libraryDependencies += "org.easymock"                 %  "easymock"                 % "3.5"     % Test
libraryDependencies += "org.cassandraunit"            %  "cassandra-unit"           % "3.3.0.2" % Test
libraryDependencies += "org.pegdown"                  %  "pegdown"                  % "1.6.0"   % Test
libraryDependencies += "com.typesafe.akka"            %% "akka-testkit"             % "2.5.11"  % Test

resolvers += Resolver.mavenLocal
resolvers += Resolver.mavenCentral

headerLicense := Some(HeaderLicense.Custom(
  """|Copyright (c) 2018 Datastax Inc.
     |
     |This software can be used solely with DataStax products. Please consult the file LICENSE.md."""
    .stripMargin
))

//
// Several integration tests start an embedded C* server.
// When the SBT shell is used and the JVM is not forked, MBean conflicts happen at the second test suite execution
// Make sure to fork the JVM so that every test suite starts from a clean state
//
Test / fork := true

//
// When building an uberjar, discard the dependencies duplicate files that are under META-INF
//
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

publishTo := Some(Resolver.file("file", new File("/Users/plaporte/env/tmp/sbt")))
releaseUseGlobalVersion := false
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  setNextVersion,
  commitNextVersion,
)

lazy val root = (project in file(".")).
  settings(
    scalaVersion := "2.12.4",
    organization := "com.datastax.gatling.plugin",
    name         := "gatling-dse-plugin"
  )
