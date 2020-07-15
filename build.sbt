import scala.sys.process._

val gatlingVersion = "2.3.0"

scalacOptions += "-target:jvm-1.8"

libraryDependencies += "com.datastax.oss"             %  "java-driver-core"              % "4.5.0"
libraryDependencies += "com.github.nscala-time"       %% "nscala-time"                   % "2.18.0"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala"          % "2.9.1"
libraryDependencies += "org.hdrhistogram"             %  "HdrHistogram"                  % "2.1.10"

libraryDependencies += "io.gatling.highcharts"        % "gatling-charts-highcharts"      % gatlingVersion % Provided

libraryDependencies += "org.fusesource"               %  "sigar"                         % "1.6.4"        % Test
libraryDependencies += "org.scalatest"                %% "scalatest"                     % "3.0.5"        % Test
libraryDependencies += "org.easymock"                 %  "easymock"                      % "3.5"          % Test
libraryDependencies += "org.cassandraunit"            %  "cassandra-unit"                % "4.3.1.0"      % Test
libraryDependencies += "org.pegdown"                  %  "pegdown"                       % "1.6.0"        % Test
libraryDependencies += "com.typesafe.akka"            %% "akka-testkit"                  % "2.5.11"       % Test
libraryDependencies += "com.datastax.oss"             %  "java-driver-query-builder"     % "4.4.0"        % Test


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

val snapshotUrl = System.getenv("publish.snapshot.url")
val releaseUrl = System.getenv("publish.release.url")
val publishUrl =
  if (isSnapshot.value) { s"$snapshotUrl;build.timestamp=" + new java.util.Date().getTime }
  else { releaseUrl }

//
// Releases should reuse credentials from other build systems.
//
// For Jenkins triggered releases, find them in the file denoted by the environment variable MAVEN_USER_SETTINGS_FILE
// If it is missing, find them in ~/.m2/settings.xml.
//
// If there is no ~/.m2/settings.xml, do not add anything to the sbt configuration.
//
val lookupM2Settings = {
  val settingsXml = sys.env.getOrElse("MAVEN_USER_SETTINGS_FILE", System.getProperty("user.home") + "/.m2/settings.xml")
  if (new File(settingsXml).exists()) {
    val mavenSettings = scala.xml.XML.loadFile(settingsXml)
    val artifactory = mavenSettings \ "servers" \ "server" filter { node => (node \ "id").text == "lab-artifactory" }
    if (artifactory.nonEmpty) {
      Seq(credentials += Credentials(
        "Artifactory Realm",
        new URI(publishUrl).getHost(),
        (artifactory \ "username").text,
        (artifactory \ "password").text))
    } else {
      Seq.empty
    }
  } else {
    Seq.empty
  }
}

publishTo := Some("Artifactory Realm" at publishUrl)

releaseUseGlobalVersion := false

lazy val repackageGatling = taskKey[Unit]("Download Gatling highcharts, add the plugin in it and repackage it")
repackageGatling := {
  val log = streams.value.log
  val downloadGatling = s"wget --quiet -O ${crossTarget.value}/gatling-charts-highcharts-bundle-$gatlingVersion-bundle.zip https://repo1.maven.org/maven2/io/gatling/highcharts/gatling-charts-highcharts-bundle/$gatlingVersion/gatling-charts-highcharts-bundle-$gatlingVersion-bundle.zip"
  val mimicZipStructure = s"mkdir -p gatling-charts-highcharts-bundle-$gatlingVersion/lib/"
  val copyUberjar = s"cp ${crossTarget.value}/gatling-dse-plugin-assembly-${version.value}.jar gatling-charts-highcharts-bundle-$gatlingVersion/lib/"
  val addUberjarInZip = s"zip -ur ${crossTarget.value}/gatling-charts-highcharts-bundle-$gatlingVersion-bundle.zip gatling-charts-highcharts-bundle-$gatlingVersion/"
  val renameBundle = s"mv ${crossTarget.value}/gatling-charts-highcharts-bundle-$gatlingVersion-bundle.zip ${crossTarget.value}/gatling-charts-highcharts-bundle-dse-plugin-$gatlingVersion-bundle.zip"
  if((downloadGatling #&& mimicZipStructure #&& copyUberjar #&& addUberjarInZip #&& renameBundle !) != 0) {
    throw new IllegalStateException("Repackaging of gatling bundle failed")
  }
}
repackageGatling := (repackageGatling dependsOn assembly).value
//publish := (publish dependsOn repackageGatling).value

lazy val root = (project in file("."))
  .settings(lookupM2Settings)
  .settings(
    scalaVersion := "2.12.4",
    organization := "com.datastax.gatling.plugin",
    name := "gatling-dse-plugin")
  .settings(
    addArtifact(
      Artifact("gatling-dse-plugin", "assembly"),
      sbtassembly.AssemblyKeys.assembly),
    addArtifact(
      Artifact(s"gatling-charts-highcharts-bundle-dse-plugin-$gatlingVersion-bundle.zip", "zip", "zip"),
      sbtassembly.AssemblyKeys.assembly))
