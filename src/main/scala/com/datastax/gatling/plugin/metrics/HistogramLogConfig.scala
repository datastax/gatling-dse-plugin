/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.metrics

import com.typesafe.config.{Config, ConfigFactory}
import io.gatling.core.Predef.configuration

import scala.collection.JavaConverters._
import scala.concurrent.duration.{Duration, FiniteDuration}

object HistogramLogConfig {
  def fromConfig(): HistogramLogConfig = {
    val conf = ConfigFactory
      .load()
      .withFallback(ConfigFactory.load("dse-plugin"))
      .withFallback(configuration.config)
      .getConfig("metrics.hgrm")
    val global: Config = conf.getConfig("globalHgrm")
    val interval: Config = conf.getConfig("globalHgrm")
    val writer: Config = conf.getConfig("logWriter")
    new HistogramLogConfig(
      enabled = conf.getBoolean("enabled"),
      directory = conf.getString("directory"),
      logTypes = conf.getStringList("logTypes").asScala.toList,
      logWriterWarmUp = writer.getDuration("warmUp"),
      logWriterDelay = writer.getDuration("delay"),
      logWriterInterval = writer.getDuration("interval"),
      globalHighest = global.getDuration("highestTrackableValue").toNanos,
      globalRes = global.getInt("resolution"),
      intervalHighest = interval.getDuration("highestTrackableValue"),
      intervalRes = interval.getInt("resolution"),
      groupLogsEnabled = conf.getBoolean("groupLogsEnabled")
    )
  }

  private implicit def asFiniteDuration(d: java.time.Duration): FiniteDuration =
    scala.concurrent.duration.Duration.fromNanos(d.toNanos)
}

class HistogramLogConfig(
    val enabled: Boolean,
    val directory: String,
    val logTypes: List[String],
    val logWriterWarmUp: Duration,
    val logWriterDelay: Duration,
    val logWriterInterval: Duration,
    val intervalHighest: Duration,
    val intervalRes: Int,
    val globalHighest: Long,
    val globalRes: Int,
    val groupLogsEnabled: Boolean)
