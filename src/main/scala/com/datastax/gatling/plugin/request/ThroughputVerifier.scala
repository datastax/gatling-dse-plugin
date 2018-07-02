/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.request

import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit.{MILLISECONDS, NANOSECONDS}
import java.util.concurrent.atomic.AtomicLong

import com.datastax.gatling.plugin.utils.GatlingTimingSource
import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.session.Session

/**
  * This class contains a single method that verifies whether the configured
  * throughput can be achieved.  To do so, it verifies if the actual send time
  * of a request is within 10 seconds of its expected send time.  If it is not,
  * an `ERROR` message is emitted so that the user knows something is wrong.
  *
  * As of 2018-06-25, the following causes are known for this message.
  *
  * (1) The feeder logic is too complex and takes too long.  This typically
  * includes I/O and heavy synchronization in the feeders.  This also include
  * cases where a `.exec { session => }` contains too complex logic.
  *
  * (2) The capacity of the client (gatling) machine is exceeded.  This case is
  * usually visible with a CPU usage close to 100% on the gatling machine, and
  * not on the target DSE cluster.
  *
  * (3) Some bad Akka tuning.
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
