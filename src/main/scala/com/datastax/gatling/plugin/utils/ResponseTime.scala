/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.utils

import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.{MILLISECONDS, NANOSECONDS}

import com.datastax.gatling.plugin.request.ThroughputVerifier
import com.typesafe.scalalogging.StrictLogging
import io.gatling.commons.util.ClockSingleton
import io.gatling.core.Predef.Session
import io.gatling.core.stats.message.ResponseTimings

trait ResponseTime {
  def latencyIn(targetTimeUnit: TimeUnit): Long

  def toGatlingResponseTimings: ResponseTimings

  def startTimeIn(targetTimeUnit: TimeUnit): Long
}

object ResponseTimeBuilder extends StrictLogging {
  val measureServiceTime: Boolean = {
    if (java.lang.Boolean.getBoolean("gatling.dse.plugin.measure_service_time")) {
      logger.info("Gatling DSE plugin is configured to measure service time")
      true
    } else {
      logger.info("Gatling DSE plugin is configured to measure response time")
      false
    }
  }

  def newResponseTimeBuilder(session: Session, gatlingTimingSource: GatlingTimingSource): ResponseTimeBuilder = {
    if (measureServiceTime) {
      // The throughput checker is useless in CO affected scenarios since throughput is not known in advance
      // Also, measuring service time MUST exclude the time spent in the feeder.
      // So it makes sense to take the "start" measurement from `nanoTime()` here.
      ServiceTime.startingAt(System.nanoTime())
    } else {
      // We trust Gatling for the user creation time when we measure response time.
      // Therefore, the response time builder can be built anywhere.
      ThroughputVerifier.checkForGatlingOverloading(session, gatlingTimingSource)
      GatlingResponseTime.startedByGatling(session, gatlingTimingSource)
    }
  }
}

trait ResponseTimeBuilder {
  def build(): ResponseTime
}

object GatlingResponseTime {
  def startedByGatling(session: Session, timingSource: TimingSource): ResponseTimeBuilder =
    () => GatlingResponseTime(session, timingSource)
}

/**
  * Gatling computes absolute timestamps in milliseconds based on the result of
  * [[System.nanoTime()]] and [[System.currentTimeMillis()]].  See the
  * implementation in [[ClockSingleton]].
  *
  * There is no technical limitation that prevents us from changing the unit of
  * these timestamps to the nanosecond.  The current (existing) timestamps can
  * just be multiplied by 1,000,000.
  *
  * However, the new timestamps in nanoseconds MUST be computed using the same
  * method than in [[ClockSingleton]].  Just relying on the result of
  * [[System.nanoTime()]] is a mistake and will never be correct.  Use the
  * method [[TimingUtils.timeSinceSessionStart()]] to get an absolute timestamp
  * in nanoseconds.
  */
case class GatlingResponseTime(session: Session, timingSource: TimingSource)
  extends ResponseTime {
  private val latencyInNanos =
    TimingUtils.timeSinceSessionStart(session, timingSource, NANOSECONDS)

  override def latencyIn(targetTimeUnit: TimeUnit): Long =
    targetTimeUnit.convert(latencyInNanos, NANOSECONDS)

  override def startTimeIn(targetTimeUnit: TimeUnit): Long =
    targetTimeUnit.convert(session.startDate, MILLISECONDS)

  override def toGatlingResponseTimings: ResponseTimings =
    ResponseTimings(
      // Gatling records durations based on absolute timestamps in milliseconds
      session.startDate,
      session.startDate + NANOSECONDS.toMillis(latencyInNanos))
}

object ServiceTime {
  def startingAt(startNanos: Long): ResponseTimeBuilder =
    () => ServiceTime(startNanos, System.nanoTime())
}

case class ServiceTime(startNanos: Long, endNanos: Long)
  extends ResponseTime {
  override def latencyIn(targetTimeUnit: TimeUnit): Long =
    targetTimeUnit.convert(endNanos - startNanos, NANOSECONDS)

  override def toGatlingResponseTimings: ResponseTimings =
    ResponseTimings(
      NANOSECONDS.toMillis(startNanos),
      NANOSECONDS.toMillis(endNanos))

  override def startTimeIn(targetTimeUnit: TimeUnit): Long =
    targetTimeUnit.convert(startNanos, NANOSECONDS)
}