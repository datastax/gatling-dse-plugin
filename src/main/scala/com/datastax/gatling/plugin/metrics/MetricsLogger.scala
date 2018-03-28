package com.datastax.gatling.plugin.metrics

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import io.gatling.core.Predef._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.io.File


trait MetricsLogger extends LazyLogging {

  private val dsePluginConf: Config = ConfigFactory.load("dse-plugin")

  protected val simName: String = configuration.core.simulationClass.get.split("\\.").last

  private val sanitizedGroups = collection.mutable.Map[String, String]()

  private val sanitizedTags = collection.mutable.Map[String, String]()

  protected val config: Config = ConfigFactory.load().withFallback(dsePluginConf)
      .withFallback(configuration.config)

  protected val metricsConfBase: String = "metrics."

  protected var tags: Set[String] = Set[String]()

  protected var groupTags: Map[String, ArrayBuffer[String]] = Map[String, ArrayBuffer[String]]()

  protected def getDirPath(dirs: String*): String = {
    dirs.mkString(File.separator)
  }

  protected def getMetricConf(metricType: String): Config = {
    config.getConfig("metrics." + metricType)
  }

  protected def sanitizeString(str: String): String = {
    str.replaceAll("[^a-zA-Z0-9.-/]", "_")
  }


  protected def getGroupId(session: Session): String = {
    if (session.groupHierarchy.isEmpty) {
      return ""
    }
    val groupConcat = session.groupHierarchy.mkString("_")
    sanitizedGroups.getOrElseUpdate(groupConcat, sanitizeString(groupConcat))
  }

  protected def getTagId(groupId: String, tag: String): String = {
    if (groupId.nonEmpty) {
      if (!groupTags.contains(groupId)) {
        groupTags += (groupId -> ArrayBuffer(tag))
      } else {
        if (!groupTags(groupId).get.contains(tag)) {
          groupTags(groupId).get.append(tag)
        }
      }
    }

    if (tag.nonEmpty && !tags.contains(tag)) {
      tags += tag
    }

    sanitizedTags.getOrElseUpdate(tag, sanitizeString(tag))
  }

}

