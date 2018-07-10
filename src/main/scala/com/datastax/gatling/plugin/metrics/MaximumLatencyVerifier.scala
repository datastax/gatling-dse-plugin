package com.datastax.gatling.plugin.metrics

import com.datastax.gatling.plugin.utils.ExponentialLogger

class MaximumLatencyVerifier(val maximumAllowedLatency: Long)
  extends ExponentialLogger(1, 10) {

  def maybeTruncateAndLog(latencyNanos: Long): Long = {
    if (latencyNanos > maximumAllowedLatency) {
      tick()
      maximumAllowedLatency
    } else {
      latencyNanos
    }
  }

  override def logTriggered(): Unit =
    logger.error("Attempt to record a latency higher than the " +
      "maximum allowed ({}ns).  Recorded latency will be truncated.  Please " +
      "adjust highestTrackableValue in dse-plugin.conf", maximumAllowedLatency)
}
