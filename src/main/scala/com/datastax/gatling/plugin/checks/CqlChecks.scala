/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.checks

import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, Row}
import com.datastax.gatling.plugin.response.CqlResponse
import com.datastax.gatling.plugin.utils.ResultSetUtils
import io.gatling.commons.validation.{SuccessWrapper, Validation}
import io.gatling.core.check._
import io.gatling.core.check.extractor.{Extractor, SingleArity}
import io.gatling.core.session.{Expression, ExpressionSuccessWrapper, Session}

import scala.collection.mutable


/**
  * This class serves as model for the CQL-specific checks.
  *
  * @param wrapped the underlying check
  */
case class DseCqlCheck(wrapped: Check[CqlResponse]) extends Check[CqlResponse] {
  override def check(response: CqlResponse, session: Session)(implicit cache: mutable.Map[Any, Any]): Validation[CheckResult] = {
    wrapped.check(response, session)
  }
}

class CqlCheckBuilder[X](extractor: Expression[Extractor[CqlResponse, X]])
  extends FindCheckBuilder[DseCqlCheck, CqlResponse, CqlResponse, X] {

  private val cqlResponseExtender: Extender[DseCqlCheck, CqlResponse] =
    wrapped => DseCqlCheck(wrapped)

  def find: ValidatorCheckBuilder[DseCqlCheck, CqlResponse, CqlResponse, X] = {
    ValidatorCheckBuilder(cqlResponseExtender, x => x.success, extractor)
  }
}

private class CqlResponseExtractor[X](val name: String,
                                      val extractor: CqlResponse => X)
  extends Extractor[CqlResponse, X] with SingleArity {

  override def apply(response: CqlResponse): Validation[Option[X]] = {
    Some(extractor.apply(response)).success
  }

  def toCheckBuilder: CqlCheckBuilder[X] = {
    new CqlCheckBuilder[X](this.expressionSuccess)
  }
}

object CqlChecks {
  val resultSet =
    new CqlResponseExtractor[AsyncResultSet](
      "resultSet",
      r => r.getCqlResultSet)
      .toCheckBuilder

  val allRows =
    new CqlResponseExtractor[Iterator[Row]](
      "allRows",
      r => ResultSetUtils.asyncResultSetToIterator(r.getCqlResultSet))
      .toCheckBuilder

  val oneRow =
    new CqlResponseExtractor[Row](
      "oneRow",
      r => ResultSetUtils.asyncResultSetToIterator(r.getCqlResultSet).next)
      .toCheckBuilder
}
