/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.metrics

import com.datastax.gatling.plugin.utils.ResponseTime
import io.gatling.core.session.Session

case class NoopMetricsLogger() extends MetricsLogger {
  override def log(session: Session, tag: String, responseTime: ResponseTime, ok: Boolean): Unit = {}

  override def close(): Unit = {}
}
