package com.datastax.gatling.plugin.metrics

import com.datastax.gatling.plugin.utils.ResponseTimers
import io.gatling.core.session.Session

case class NoopMetricsLogger() extends MetricsLogger {
  override def log(session: Session, tag: String, responseTimers: ResponseTimers, ok: Boolean): Unit = {}

  override def close(): Unit = {}
}
