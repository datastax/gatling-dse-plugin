package com.datastax.gatling.plugin.checks

import com.datastax.gatling.plugin.response.DseResponse
import io.gatling.commons.validation.Validation
import io.gatling.core.check.{Check, CheckResult}
import io.gatling.core.session.Session

import scala.collection.mutable

/**
  * This class serves as model for the CQL-specific checks
  *
  * @param wrapped the underlying check
  */
case class DseCheck(wrapped: Check[DseResponse]) extends Check[DseResponse] {

  override def check(response: DseResponse, session: Session)(implicit cache: mutable.Map[Any, Any]): Validation[CheckResult] = {
    wrapped.check(response, session)
  }
}

