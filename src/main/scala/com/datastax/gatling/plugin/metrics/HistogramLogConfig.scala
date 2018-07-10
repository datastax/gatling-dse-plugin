/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.metrics

import java.util.concurrent.TimeUnit

import com.typesafe.config.{Config, ConfigFactory}
import io.gatling.core.Predef.configuration

import scala.util.Try

object HistogramLogConfig {
  def fromConfig(): HistogramLogConfig = {
    val histogramsConfig: Config = ConfigFactory.load()
      .withFallback(ConfigFactory.load("dse-plugin"))
      .withFallback(configuration.config)
      .getConfig("metrics.hgrm")
    HistogramLogConfig(
      histogramsConfig.getBoolean("enabled"),
      histogramsConfig.getString("directory"),
      histogramsConfig.getDuration("logWriter.warmUp"),
      histogramsConfig.getDuration("logWriter.delay"),
      histogramsConfig.getDuration("logWriter.interval"),
      categoryConfig(histogramsConfig, "query", "default"),
      categoryConfig(histogramsConfig, "group", "query"),
      categoryConfig(histogramsConfig, "global", "default")
    )
  }

  /**
    * Builds a histogram category configuration for the given category.
    * If values are missing, the values of the provided defaultCategory (except
    * `enabled` are used).
    */
  private def categoryConfig(config: Config,
                             mainCategory: String,
                             fallbackCategory: String) = {
    val main = config.getConfig(mainCategory)
    val fallback = config.getConfig(fallbackCategory)
    val enabled: Boolean =
      Try(main.getBoolean("enabled"))
        .getOrElse(false)
    val highestValue: Long =
      Try(main.getDuration("highestTrackableValue"))
        .orElse(Try(fallback.getDuration("highestTrackableValue")))
        .map(d => d.toNanos)
        .getOrElse(TimeUnit.MINUTES.toNanos(5))
    val resolution: Int =
      Try(main.getInt("resolution"))
        .orElse(Try(fallback.getInt("resolution")))
        .getOrElse(3)
    HistogramCategoryConfig(enabled, highestValue, resolution)
  }
}

case class HistogramLogConfig(enabled: Boolean,
                              directory: String,
                              logWriterWarmUp: java.time.Duration,
                              logWriterDelay: java.time.Duration,
                              logWriterInterval: java.time.Duration,
                              queryHistograms: HistogramCategoryConfig,
                              groupHistograms: HistogramCategoryConfig,
                              globalHistograms: HistogramCategoryConfig)

case class HistogramCategoryConfig(enabled: Boolean,
                                   highestValue: Long,
                                   resolution: Int) {
  val maximumLatencyVerifier = new MaximumLatencyVerifier(highestValue)
}
