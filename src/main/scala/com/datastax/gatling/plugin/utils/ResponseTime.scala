/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.utils

import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.{MILLISECONDS, NANOSECONDS}

import io.gatling.commons.util.ClockSingleton
import io.gatling.core.Predef.Session
import io.gatling.core.stats.message.ResponseTimings

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
case class ResponseTime(session: Session, timingSource: TimingSource) {
  private val latencyInNanos =
    TimingUtils.timeSinceSessionStart(session, timingSource, NANOSECONDS)

  def latencyIn(targetTimeUnit: TimeUnit): Long =
    targetTimeUnit.convert(latencyInNanos, NANOSECONDS)

  def startTimeInSeconds: Long =
    MILLISECONDS.toSeconds(session.startDate)

  def toGatlingResponseTimings: ResponseTimings =
    ResponseTimings(
      // Gatling records durations based on absolute timestamps in milliseconds
      session.startDate,
      session.startDate + NANOSECONDS.toMillis(latencyInNanos))
}