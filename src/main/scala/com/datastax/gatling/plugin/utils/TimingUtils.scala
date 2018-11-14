/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.utils

import java.util.concurrent.TimeUnit.{MILLISECONDS, NANOSECONDS}

import io.gatling.commons.util.ClockSingleton
import io.gatling.core.Predef.Session

import scala.concurrent.duration.TimeUnit

/**
  * This class is a hack that allows to measure latency in nanoseconds.
  *
  */
// TODO: give back the improvements in this class to Gatling by submitting a PR to add `DefaultClock::computeTimeFromNanos(nanos: Long, targetUnit: TimeUnit)`
object TimingUtils {
  def timeSinceSessionStart(session: Session,
                            timingSource: TimingSource,
                            targetTimeUnit: TimeUnit = NANOSECONDS): Long = {
    val currentTimeNanos = timingSource.currentTimeNanos()
    val startTimeNanos = MILLISECONDS.toNanos(session.startDate)
    val elapsedNanosSinceStart = currentTimeNanos - startTimeNanos
    targetTimeUnit.convert(elapsedNanosSinceStart, NANOSECONDS)
  }
}

trait TimingSource {
  def currentTimeNanos(): Long
}

case class GatlingTimingSource() extends TimingSource {
  private val gatlingInternalClock =
    unlock(ClockSingleton, "_clock")

  private val gatlingNanoTimeReference =
    unlock(gatlingInternalClock, "nanoTimeReference")
      .asInstanceOf[Long]

  private val gatlingStartTimeInNanos = MILLISECONDS.toNanos(
    unlock(gatlingInternalClock, "currentTimeMillisReference")
      .asInstanceOf[Long])

  private def unlock(o: AnyRef, declaredMethodName: String) = {
    val method = o.getClass.getDeclaredMethod(declaredMethodName)
    method.setAccessible(true)
    method.invoke(o)
  }

  // Current time in nanos = JVM absolute startup time in nanos inferred from currentTimeMillis + (elapsed nanoseconds
  // since JVM startup)
  override def currentTimeNanos(): Long =
    gatlingStartTimeInNanos + (System.nanoTime() - gatlingNanoTimeReference)
}
