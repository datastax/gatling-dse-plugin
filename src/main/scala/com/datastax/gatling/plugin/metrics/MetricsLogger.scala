/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.metrics

import java.io.Closeable
import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import com.datastax.gatling.plugin.utils.ResponseTimers
import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.session.Session

import scala.concurrent.ExecutionContextExecutor
import scala.concurrent.duration.Duration

trait MetricsLogger extends Closeable {
  def log(session: Session, tag: String, responseTimers: ResponseTimers, ok: Boolean): Unit
}

object MetricsLogger extends StrictLogging {
  def newMetricsLogger(actorSystem: ActorSystem, startEpoch: Long): MetricsLogger = {
    val histogramLogConfig = HistogramLogConfig.fromConfig()
    if (histogramLogConfig.enabled) {
      logger.info("HDRHistogram results recording is enabled except for the first {}s of the run", histogramLogConfig.logWriterWarmUp.toSeconds)
      logger.info("Starting flushing actor with delay {}s and interval {}s",
        histogramLogConfig.logWriterDelay.toSeconds,
        histogramLogConfig.logWriterInterval.toSeconds)

      val histogramLogger = new HistogramLogger(startEpoch)
      implicit val executor: ExecutionContextExecutor = actorSystem.dispatcher
      actorSystem.scheduler.schedule(
        initialDelay = Duration(histogramLogConfig.logWriterDelay.toSeconds, TimeUnit.SECONDS),
        interval = Duration(histogramLogConfig.logWriterInterval.toSeconds, TimeUnit.SECONDS),
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