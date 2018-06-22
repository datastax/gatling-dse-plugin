package com.datastax.gatling.plugin.request

import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit.{MILLISECONDS, NANOSECONDS}
import java.util.concurrent.atomic.AtomicLong

import com.datastax.gatling.plugin.utils.GatlingTimingSource
import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.session.Session

/**
  * This class contains a single method that verifies if
  */
object ThroughputVerifier extends StrictLogging {
  private val warningThresholdInSec = 10
  private val warningSemaphore = new Semaphore(1)
  private val spottedDelays = new AtomicLong()
  @volatile private var nextWarning = 1000

  def checkForGatlingOverloading(session: Session,
                                 timeSource: GatlingTimingSource): Unit = {
    val userStartTimeSec = MILLISECONDS.toSeconds(session.startDate)
    val currentTimeSec = NANOSECONDS.toSeconds(timeSource.currentTimeNanos())
    if (currentTimeSec - userStartTimeSec > warningThresholdInSec) {
      val spotted = spottedDelays.incrementAndGet()
      if (spotted > nextWarning) {
        if (warningSemaphore.tryAcquire()) {
          logger.error("Gatling plugin cannot keep up with the " +
            "target injection rate. {} queries have been sent more than {}s " +
            "after they expected send time.  This can be caused by the query " +
            "preparation taking too long.  Check the CPU usage on the " +
            "Gatling machine and the feeders code.  Reducing the target " +
            "injection rate may also help.", spotted, warningThresholdInSec)
          nextWarning *= 2
          warningSemaphore.release()
        }
      }
    }
  }
}
