/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.metrics

import java.io.Closeable
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.MILLISECONDS

import akka.actor.ActorSystem
import com.datastax.gatling.plugin.utils.ResponseTime
import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.session.Session

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.Duration

trait MetricsLogger extends Closeable {
  def log(session: Session, tag: String, responseTime: ResponseTime, ok: Boolean): Unit
}

object MetricsLogger extends StrictLogging {
  def newMetricsLogger(actorSystem: ActorSystem, startEpoch: Long): MetricsLogger = {
    val config = HistogramLogConfig.fromConfig()
    if (config.enabled) {
      logger.info("HDRHistogram results recording is enabled")
      logger.info("Starting flushing actor with delay {}s and interval {}s",
        MILLISECONDS.toSeconds(config.logWriterDelay.toMillis),
        MILLISECONDS.toSeconds(config.logWriterInterval.toMillis))

      val histogramLogger = new HistogramLogger(startEpoch)
      implicit val executor: ExecutionContextExecutor = actorSystem.dispatcher
      actorSystem.scheduler.schedule(
        initialDelay = Duration(config.logWriterDelay.toMillis, MILLISECONDS),
        interval = Duration(config.logWriterInterval.toMillis, MILLISECONDS),
        runnable = () => histogramLogger.writeNewData()
      )
      histogramLogger
    } else {
      logger.info("HDRHistogram results recording is disabled")
      NoopMetricsLogger()
    }
  }

  def sanitizeString(str: String): String = {
    str.replaceAll("[^a-zA-Z0-9.-/]", "_")
  }
}