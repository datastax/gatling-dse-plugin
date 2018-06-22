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

//
// Releases should reuse credentials from other build systems
//
// For Jenkins triggered releases, find them in the file denoted by the environment variable MAVEN_USER_SETTINGS_FILE
// If it is missing, find them in ~/.m2/settings.xml
//
val settingsXml = sys.env.getOrElse("MAVEN_USER_SETTINGS_FILE", System.getProperty("user.home") + "/.m2/settings.xml")
val mavenSettings = scala.xml.XML.loadFile(settingsXml)
val artifactory = mavenSettings \ "servers" \ "server" filter { node => (node \ "id").text == "artifactory" }
publishTo := {
  if (isSnapshot.value) {
    Some("Artifactory Realm" at "http://datastax.jfrog.io/datastax/datastax-public-snapshots-local;" +
      "build.timestamp=" + new java.util.Date().getTime)
  } else {
    Some("Artifactory Realm" at "http://datastax.jfrog.io/datastax/datastax-public-releases-local")
  }
}
credentials += Credentials(
  "Artifactory Realm",
  "datastax.jfrog.io",
  (artifactory \ "username").text,
  (artifactory \ "password").text)
releaseUseGlobalVersion := false

lazy val root = (project in file(".")).
  settings(
    scalaVersion := "2.12.4",
    organization := "com.datastax.gatling.plugin",
    name         := "gatling-dse-plugin"
  ).
  settings(
    addArtifact(
      Artifact("gatling-dse-plugin", "assembly"),
      sbtassembly.AssemblyKeys.assembly))
