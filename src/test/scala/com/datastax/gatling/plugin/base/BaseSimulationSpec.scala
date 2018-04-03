package com.datastax.gatling.plugin.base

import java.io.File

import io.gatling.commons.util.PathHelper._
import io.gatling.core.config.GatlingPropertiesBuilder


abstract class BaseSimulationSpec extends BaseCassandraServerSpec {

  val cqlSimulationDir = "com.datastax.gatling.plugin.simulations.cql."

  val graphSimulationDir = "com.datastax.gatling.plugin.simulations.graph."

  val projectRootDir = new File(".").toPath

  val mavenResourcesDirectory = projectRootDir / "src" / "test" / "resources"
  val mavenTargetDirectory = projectRootDir / "target"

  val dataDirectory = mavenResourcesDirectory / "data"
  val bodiesDirectory = mavenResourcesDirectory / "bodies"

  val mavenBinariesDirectory = mavenTargetDirectory / "test-classes"
  val resultsDirectory = mavenTargetDirectory / "results"

  def getGatlingProps = {
    val props = new GatlingPropertiesBuilder

    props.mute()
    props.noReports()
    props.dataDirectory(dataDirectory.toString)
    props.resultsDirectory(resultsDirectory.toString)
    props.bodiesDirectory(bodiesDirectory.toString)
    props.binariesDirectory(mavenBinariesDirectory.toString)
    props
  }

}
