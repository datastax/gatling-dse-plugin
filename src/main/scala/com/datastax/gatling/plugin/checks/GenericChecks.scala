/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.checks

import java.nio.ByteBuffer

import com.datastax.gatling.plugin.response.DseResponse
import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.datastax.oss.driver.api.core.cql._
import com.datastax.oss.driver.api.core.metadata.Node
import io.gatling.commons.validation.{SuccessWrapper, Validation}
import io.gatling.core.check.extractor.{Extractor, SingleArity}
import io.gatling.core.check._
import io.gatling.core.session.{Expression, ExpressionSuccessWrapper, Session}

import scala.collection.mutable

/**
  * This class allows to execute checks on either CQL or Graph responses.
  *
  * There is no class hierarchy between DseResponseCheck and DseCqlCheck or
  * DseGraphCheck on purpose.  Otherwise any method that accept DseResponseCheck
  * would make it possible to execute CQL checks on Graph responses, or
  * vice-versa.
  */
case class GenericCheck(wrapped: Check[DseResponse]) extends Check[DseResponse] {
  override def check(response: DseResponse, session: Session)(implicit cache: mutable.Map[Any, Any]): Validation[CheckResult] = {
    wrapped.check(response, session)
  }
}

class GenericCheckBuilder[X](extractor: Expression[Extractor[DseResponse, X]])
  extends FindCheckBuilder[GenericCheck, DseResponse, DseResponse, X] {

  private val dseResponseExtender: Extender[GenericCheck, DseResponse] =
    wrapped => GenericCheck(wrapped)

  def find: ValidatorCheckBuilder[GenericCheck, DseResponse, DseResponse, X] = {
    ValidatorCheckBuilder(dseResponseExtender, x => x.success, extractor)
  }
}

private class GenericResponseExtractor[X](val name: String,
                                          val extractor: DseResponse => X)
  extends Extractor[DseResponse, X] with SingleArity {

  override def apply(response: DseResponse): Validation[Option[X]] = {
    Some(extractor.apply(response)).success
  }

  def toCheckBuilder: GenericCheckBuilder[X] = {
    new GenericCheckBuilder[X](this.expressionSuccess)
  }
}

object GenericChecks {
  val executionInfo =
    new GenericResponseExtractor[ExecutionInfo](
      "executionInfo",
      r => r.executionInfo())
      .toCheckBuilder

  val speculativeExecutionsExtractor =
    new GenericResponseExtractor[Int](
      "speculativeExecutions",
      r => r.speculativeExecutions())
      .toCheckBuilder

  val pagingState =
    new GenericResponseExtractor[ByteBuffer](
      "pagingState",
      r => r.pagingState())
      .toCheckBuilder

  val warnings =
    new GenericResponseExtractor[List[String]](
      "warnings",
      r => r.warnings())
      .toCheckBuilder

  val successfulExecutionIndex =
    new GenericResponseExtractor[Int](
      "successfulExecutionIndex",
      r => r.successFullExecutionIndex())
      .toCheckBuilder

  val schemaInAgreement =
    new GenericResponseExtractor[Boolean](
      "schemaInAgreement",
      r => r.schemaInAgreement())
      .toCheckBuilder

  val rowCount =
    new GenericResponseExtractor[Int](
      "rowCount",
      r => r.rowCount())
      .toCheckBuilder

  val applied =
    new GenericResponseExtractor[Boolean](
      "applied",
      r => r.applied())
      .toCheckBuilder

  val exhausted =
    new GenericResponseExtractor[Option[Boolean]](
      "exhausted",
      r => r.exhausted())
      .toCheckBuilder

  val coordinator =
    new GenericResponseExtractor[Node](
      "queriedHost",
      r => r.coordinator())
      .toCheckBuilder
}

