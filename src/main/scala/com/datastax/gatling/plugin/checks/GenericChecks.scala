/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.checks

import com.datastax.oss.driver.api.core.cql._
import com.datastax.oss.driver.api.core._
import com.datastax.oss.driver.api.core.metadata._
import com.datastax.gatling.plugin.response.DseResponse
import io.gatling.commons.validation.{SuccessWrapper, Validation}
import io.gatling.core.check.extractor.{Extractor, SingleArity}
import io.gatling.core.check._
import io.gatling.core.session.{Expression, ExpressionSuccessWrapper, Session}

import scala.collection.mutable
import java.nio.ByteBuffer

import com.datastax.dse.driver.api.core.graph.GraphExecutionInfo

/**
  * This class allows to execute checks on either CQL or Graph responses.
  *
  * There is no class hierarchy between DseResponseCheck and DseCqlCheck or
  * DseGraphCheck on purpose.  Otherwise any method that accept DseResponseCheck
  * would make it possible to execute CQL checks on Graph responses, or
  * vice-versa.
  */
case class GenericCheck[E](wrapped: Check[DseResponse[E]]) extends Check[DseResponse[E]] {
  override def check(response: DseResponse[E], session: Session)(implicit cache: mutable.Map[Any, Any]): Validation[CheckResult] = {
    wrapped.check(response, session)
  }
}

class GenericCheckBuilder[X, E](extractor: Expression[Extractor[DseResponse[E], X]])
  extends FindCheckBuilder[GenericCheck[E], DseResponse[E], DseResponse[E], X] {

  private val dseResponseExtender: Extender[GenericCheck[E], DseResponse[E]] =
    wrapped => GenericCheck(wrapped)

  def find: ValidatorCheckBuilder[GenericCheck[E], DseResponse[E], DseResponse[E], X] = {
    ValidatorCheckBuilder(dseResponseExtender, x => x.success, extractor)
  }
}

private class GenericResponseExtractor[X, E](val name: String,
                                          val extractor: DseResponse[E] => X)
  extends Extractor[DseResponse[E], X] with SingleArity {

  override def apply(response: DseResponse[E]): Validation[Option[X]] = {
    Some(extractor.apply(response)).success
  }

  def toCheckBuilder: GenericCheckBuilder[X, E] = {
    new GenericCheckBuilder[X, E](this.expressionSuccess)
  }
}

class GenericChecks[T] {
  val executionInfo =
    new GenericResponseExtractor[T, T](
      "executionInfo",
      r => r.executionInfo())
      .toCheckBuilder

  val queriedHost =
    new GenericResponseExtractor[Node, T](
      "queriedHost",
      r => r.queriedHost())
      .toCheckBuilder

  val achievedConsistencyLevel =
    new GenericResponseExtractor[ConsistencyLevel, T](
      "achievedConsistencyLevel",
      r => r.getConsistencyLevel())
      .toCheckBuilder

  val speculativeExecutionsExtractor =
    new GenericResponseExtractor[Int, T](
      "speculativeExecutions",
      r => r.speculativeExecutions())
      .toCheckBuilder

  val pagingState =
    new GenericResponseExtractor[ByteBuffer, T](
      "pagingState",
      r => r.pagingState())
      .toCheckBuilder

  val triedHosts =
    new GenericResponseExtractor[List[Node], T](
      "triedHost",
      r => r.triedHosts())
      .toCheckBuilder

  val warnings =
    new GenericResponseExtractor[List[String], T](
      "warnings",
      r => r.warnings())
      .toCheckBuilder

  val successfulExecutionIndex =
    new GenericResponseExtractor[Int, T](
      "successfulExecutionIndex",
      r => r.successFullExecutionIndex())
      .toCheckBuilder

  val schemaInAgreement =
    new GenericResponseExtractor[Boolean, T](
      "schemaInAgreement",
      r => r.schemaInAgreement())
      .toCheckBuilder

  val rowCount =
    new GenericResponseExtractor[Int, T](
      "rowCount",
      r => r.rowCount())
      .toCheckBuilder

  val applied =
    new GenericResponseExtractor[Boolean, T](
      "applied",
      r => r.applied())
      .toCheckBuilder

  val exhausted =
    new GenericResponseExtractor[Boolean, T](
      "exhausted",
      r => r.applied())
      .toCheckBuilder
}

object CqlGenericChecks extends GenericChecks[ExecutionInfo];
object GraphGenericChecks extends GenericChecks[GraphExecutionInfo];
