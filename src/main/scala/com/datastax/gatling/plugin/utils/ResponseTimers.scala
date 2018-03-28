package com.datastax.gatling.plugin.utils

import java.lang.System.{currentTimeMillis, nanoTime}
import java.util.concurrent.TimeUnit

import io.gatling.core.stats.message.ResponseTimings

/**
  * Response Time class
  *
  * @param startTimes tuple of start time in nano and milliseconds
  */
case class ResponseTimers(startTimes: (Long, Long)) {

  val diffMicros: Long = (nanoTime - startTimes._1) / 1000

  val diffNanos: Long = nanoTime - startTimes._1

  val currentMillis = currentTimeMillis

  val diffMillis = currentTimeMillis - startTimes._2

  def responseTimings = ResponseTimings(startTimes._2, currentTimeMillis)

  def getNanosAndMs: (Long, Long) = (nanoTime, currentTimeMillis)

  def getStartTimeInSeconds = TimeUnit.MILLISECONDS.toSeconds(startTimes._2)

}
