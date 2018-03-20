package com.datastax.gatling.plugin.metrics

import com.typesafe.config.ConfigFactory
import io.gatling.core.Predef.configuration

import scala.collection.JavaConverters._
import scala.concurrent.duration.{Duration, FiniteDuration}

object HistogramLogConfig {
  protected final val metricType = "hgrm"
  protected val metricsConfBase: String = "metrics."

  def fromConfig(): HistogramLogConfig = {
    val config = ConfigFactory.load()
      .withFallback(ConfigFactory.load("dse-plugin"))
      .withFallback(configuration.config)
    new HistogramLogConfig(
      enabled = config.getConfig(metricsConfBase + metricType).getBoolean("enabled"),
      directory = config.getConfig(metricsConfBase + metricType).getString("directory"),
      logTypes = config.getConfig(metricsConfBase + metricType).getStringList("logTypes").asScala.toList,
      logWriterWarmUp = config.getConfig(metricsConfBase + metricType).getDuration("logWriter.warmUp"),
      logWriterDelay = config.getConfig(metricsConfBase + metricType).getDuration("logWriter.delay"),
      logWriterInterval = config.getConfig(metricsConfBase + metricType).getDuration("logWriter.interval"),
      globalHighest = config.getConfig(metricsConfBase + metricType).getDuration("globalHgrm.highestTrackableValue").toNanos,
      globalRes = config.getConfig(metricsConfBase + metricType).getInt("globalHgrm.resolution"),
      intervalHighest = config.getConfig(metricsConfBase + metricType).getDuration("intervalHgrm.highestTrackableValue"),
      intervalRes = config.getConfig(metricsConfBase + metricType).getInt("intervalHgrm.resolution"),
      groupLogsEnabled = config.getConfig(metricsConfBase + metricType).getBoolean("groupLogsEnabled")
    )
  }

  private implicit def asFiniteDuration(d: java.time.Duration): FiniteDuration = {
    scala.concurrent.duration.Duration.fromNanos(d.toNanos)
  }
}

class HistogramLogConfig(val enabled: Boolean,
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