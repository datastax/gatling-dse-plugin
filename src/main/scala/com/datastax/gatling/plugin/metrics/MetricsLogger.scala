package com.datastax.gatling.plugin.metrics

import java.util
import java.util.Collections
import java.util.concurrent.{ConcurrentMap, ConcurrentSkipListMap}

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import io.gatling.core.Predef._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.io.File


trait MetricsLogger extends LazyLogging {

  private val dsePluginConf: Config = ConfigFactory.load("dse-plugin")

  protected val simName: String = configuration.core.simulationClass.get.split("\\.").last

  private val sanitizedGroups: ConcurrentMap[String, String] = new ConcurrentSkipListMap[String, String]()

  private val sanitizedTags: ConcurrentMap[String, String] = new ConcurrentSkipListMap[String, String]()

  protected val config: Config = ConfigFactory.load().withFallback(dsePluginConf)
    .withFallback(configuration.config)

  protected val metricsConfBase: String = "metrics."

  protected val tags: util.Set[String] = Collections.newSetFromMap(new ConcurrentSkipListMap[String, java.lang.Boolean]())

  protected val groupTags: ConcurrentMap[String, ArrayBuffer[String]] = new ConcurrentSkipListMap[String, ArrayBuffer[String]]()

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
      ""
    } else {
      sanitizedGroups.computeIfAbsent(session.groupHierarchy.mkString("_"), sanitizeString)
    }
  }

  protected def getTagId(groupId: String, tag: String): String = {
    if (groupId.nonEmpty) {
      if (!groupTags.containsKey(groupId)) {
        groupTags.put(groupId, ArrayBuffer(tag))
      } else {
        if (!groupTags.get(groupId).contains(tag)) {
          groupTags.get(groupId).append(tag)
        }
      }
    }

    if (tag.nonEmpty && !tags.contains(tag)) {
      tags.add(tag)
    }

    sanitizedTags.computeIfAbsent(tag, sanitizeString)
  }
}

