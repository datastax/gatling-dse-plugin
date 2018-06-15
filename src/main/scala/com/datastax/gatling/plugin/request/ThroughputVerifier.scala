package com.datastax.gatling.plugin.request

import java.util.concurrent.TimeUnit.{MILLISECONDS, NANOSECONDS}
import java.util.concurrent.atomic.AtomicLong

import com.datastax.gatling.plugin.utils.GatlingTimingSource
import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.session.Session

object ThroughputVerifier extends StrictLogging {
  private val spottedDelays = new AtomicLong()
  private val warningThresholdInSec = 10
  private val numberOfDelaysBeforeNextWarning = 10 * 1000

  def checkForGatlingOverloading(session: Session,
                                 timeSource: GatlingTimingSource): Unit = {
    val userStartTimeSec = MILLISECONDS.toSeconds(session.startDate)
    val currentTimeSec = NANOSECONDS.toSeconds(timeSource.currentTimeNanos())
    if (currentTimeSec - userStartTimeSec > warningThresholdInSec) {
      val delaysSoFar = spottedDelays.incrementAndGet()
      if (delaysSoFar % numberOfDelaysBeforeNextWarning == 0) {
        logger.error("Gatling cannot keep up with the target injection rate. " +
          "{} queries have been sent more than {}s after they expected send " +
          "time.  Check the CPU usage on the Gatling server and reduce the " +
          "target injection rate.", delaysSoFar, warningThresholdInSec)
      }
    }
  }
}
