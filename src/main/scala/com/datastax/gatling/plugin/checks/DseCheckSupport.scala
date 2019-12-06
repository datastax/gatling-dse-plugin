/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.checks

import io.gatling.core.session.ExpressionSuccessWrapper

trait DseCheckSupport {

  // execution info and subsets
  lazy val executionInfo = GenericChecks.executionInfo
  lazy val pagingState = GenericChecks.pagingState
  lazy val schemaAgreement = GenericChecks.schemaInAgreement
  lazy val successfulExecutionIndex = GenericChecks.successfulExecutionIndex
  lazy val warnings = GenericChecks.warnings
  lazy val coordinator = GenericChecks.coordinator

  // start CQL only checks
  lazy val resultSet = CqlChecks.resultSet
  lazy val allRows = CqlChecks.allRows
  lazy val oneRow = CqlChecks.oneRow

  // start Graph only checks
  lazy val graphResultSet = GraphChecks.graphResultSet
  lazy val allNodes = GraphChecks.allNodes
  lazy val oneNode = GraphChecks.oneNode

  def edges(columnName: String) = GraphChecks.edges(columnName)

  def vertexes(columnName: String) = GraphChecks.vertexes(columnName)

  def paths(columnName: String) = GraphChecks.paths(columnName)

  def properties(columnName: String) = GraphChecks.paths(columnName)

  def vertexProperties(columnName: String) = GraphChecks.vertexProperties(columnName)
}

