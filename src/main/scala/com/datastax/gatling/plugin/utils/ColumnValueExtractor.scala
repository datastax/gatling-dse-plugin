/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.utils

import com.datastax.driver.core.ResultSet
import com.datastax.driver.dse.graph.GraphResultSet
import com.datastax.gatling.plugin.response.DseResponse
import io.gatling.commons.validation.{SuccessWrapper, Validation}
import io.gatling.core.check.extractor._


abstract class ColumnValueExtractor[X] extends CriterionExtractor[DseResponse, Any, X] {
  val criterionName = "columnValue"
}

class SingleColumnValueExtractor(val criterion: String, val occurrence: Int) extends ColumnValueExtractor[Any] with FindArity {
  def extract(response: DseResponse): Validation[Option[Any]] = {
    response.resultSet match {
      case _: ResultSet => response.getCqlResultColumnValues(criterion).lift(occurrence).success
      case _: GraphResultSet => response.getGraphResultColumnValues(criterion).lift(occurrence).success
    }
  }
}

class MultipleColumnValueExtractor(val criterion: String) extends ColumnValueExtractor[Seq[Any]] with FindAllArity {
  def extract(response: DseResponse): Validation[Option[Seq[Any]]] = {
    response.resultSet match {
      case _: ResultSet => response.getCqlResultColumnValues(criterion).liftSeqOption.success
      case _: GraphResultSet => response.getGraphResultColumnValues(criterion).liftSeqOption.success
    }
  }
}

class CountColumnValueExtractor(val criterion: String) extends ColumnValueExtractor[Int] with CountArity {
  def extract(response: DseResponse): Validation[Option[Int]] = {
    response.resultSet match {
      case _: ResultSet => response.getCqlResultColumnValues(criterion).liftSeqOption.map(_.size).success
      case _: GraphResultSet => response.getGraphResultColumnValues(criterion).liftSeqOption.map(_.size).success
    }
  }
}
