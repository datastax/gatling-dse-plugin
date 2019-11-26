/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.checks

import com.datastax.oss.driver.api.core.cql.{ResultSet, Row}
import com.datastax.gatling.plugin.response.CqlResponse
import io.gatling.commons.validation.{SuccessWrapper, Validation}
import io.gatling.core.check._
import io.gatling.core.check.extractor.{CountArity, CriterionExtractor, Extractor, FindAllArity, FindArity, SingleArity, _}
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

private abstract class ColumnValueExtractor[X] extends CriterionExtractor[CqlResponse, Any, X] {
  val criterionName = "columnValue"
}

private class SingleColumnValueExtractor(val criterion: String, val occurrence: Int) extends ColumnValueExtractor[Any] with FindArity {
  def extract(response: CqlResponse): Validation[Option[Any]] =
    response.getColumnValSeq(criterion).lift(occurrence).success
}

private class MultipleColumnValueExtractor(val criterion: String) extends ColumnValueExtractor[Seq[Any]] with FindAllArity {
  def extract(response: CqlResponse): Validation[Option[Seq[Any]]] =
    response.getColumnValSeq(criterion).liftSeqOption.success
}

private class CountColumnValueExtractor(val criterion: String) extends ColumnValueExtractor[Int] with CountArity {
  def extract(response: CqlResponse): Validation[Option[Int]] =
    response.getColumnValSeq(criterion).liftSeqOption.map(_.size).success
}

object CqlChecks {
  val resultSet =
    new CqlResponseExtractor[ResultSet](
      "resultSet",
      r => r.getCqlResultSet)
      .toCheckBuilder

  val allRows =
    new CqlResponseExtractor[Seq[Row]](
      "allRows",
      r => r.getAllRowsSeq)
      .toCheckBuilder

  val oneRow =
    new CqlResponseExtractor[Row](
      "oneRow",
      r => r.getOneRow)
      .toCheckBuilder

  def columnValue(columnName: Expression[String]) = {
    val cqlResponseExtender: Extender[DseCqlCheck, CqlResponse] = wrapped => DseCqlCheck(wrapped)
    new DefaultMultipleFindCheckBuilder[DseCqlCheck, CqlResponse, CqlResponse, Any](cqlResponseExtender, x => x.success) {
      def findExtractor(occurrence: Int) = columnName.map(new SingleColumnValueExtractor(_, occurrence))
      def findAllExtractor = columnName.map(new MultipleColumnValueExtractor(_))
      def countExtractor = columnName.map(new CountColumnValueExtractor(_))
    }
  }
}
