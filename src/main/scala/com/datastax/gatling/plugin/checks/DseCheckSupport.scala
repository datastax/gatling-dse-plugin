/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.checks

import java.nio.ByteBuffer

import com.datastax.driver.core._
import com.datastax.driver.dse.graph._
import com.datastax.gatling.plugin.checks.DseCheckBuilders._
import com.datastax.gatling.plugin.response.DseResponse
import com.datastax.gatling.plugin.utils.{
  CountColumnValueExtractor,
  MultipleColumnValueExtractor,
  SingleColumnValueExtractor
}
import io.gatling.core.check._
import io.gatling.core.session.{Expression, RichExpression}

trait DseCheckSupport {

  // start global checks
  lazy val exhausted: DseCheckBuilder[Boolean] =
    DseChecks.exhausted
  lazy val applied: DseCheckBuilder[Boolean] =
    DseChecks.applied
  lazy val rowCount: DseCheckBuilder[Int] =
    DseChecks.rowCount

  // execution info and subsets
  lazy val executionInfo: DseCheckBuilder[ExecutionInfo] =
    DseChecks.executionInfo
  lazy val achievedCL: DseCheckBuilder[ConsistencyLevel] =
    DseChecks.achievedConsistencyLevel
  lazy val statement: DseCheckBuilder[Statement] =
    DseChecks.statement
  lazy val incomingPayload: DseCheckBuilder[Map[String, ByteBuffer]] =
    DseChecks.incomingPayload
  lazy val pagingState: DseCheckBuilder[PagingState] =
    DseChecks.pagingState
  lazy val queriedHost: DseCheckBuilder[Host] =
    DseChecks.queriedHost
  lazy val schemaAgreement: DseCheckBuilder[Boolean] =
    DseChecks.schemaInAgreement
  lazy val speculativeExecutions: DseCheckBuilder[Int] =
    DseChecks.speculativeExecutions
  lazy val successfulExecutionIndex: DseCheckBuilder[Int] =
    DseChecks.successfulExecutionIndex
  lazy val triedHosts: DseCheckBuilder[List[Host]] =
    DseChecks.triedHosts
  lazy val warnings: DseCheckBuilder[List[String]] =
    DseChecks.warnings

  // start CQL only checks
  lazy val resultSet: DseCheckBuilder[ResultSet] =
    DseChecks.resultSet
  lazy val allRows: DseCheckBuilder[Seq[Row]] =
    DseChecks.allRows
  lazy val oneRow: DseCheckBuilder[Row] =
    DseChecks.oneRow

  // start Graph only checks
  lazy val graphResultSet: DseCheckBuilder[GraphResultSet] =
    DseChecks.graphResultSet
  lazy val allNodes: DseCheckBuilder[Seq[GraphNode]] =
    DseChecks.allNodes
  lazy val oneNode: DseCheckBuilder[GraphNode] =
    DseChecks.oneNode
  def edges(name: String): DseCheckBuilder[Seq[Edge]] =
    DseChecks.edges(name)
  def vertexes(name: String): DseCheckBuilder[Seq[Vertex]] =
    DseChecks.vertices(name)
  def paths(name: String): DseCheckBuilder[Seq[Path]] =
    DseChecks.paths(name)
  def properties(name: String): DseCheckBuilder[Seq[Property]] =
    DseChecks.properties(name)
  def vertexProperties(name: String): DseCheckBuilder[Seq[VertexProperty]] =
    DseChecks.vertexProperties(name)

  /**
    * Get a column by name returned by the CQL statement.
    * Note that this statement implicitly fetches <b>all</b> rows from the result set!
    */
  def columnValue(columnName: Expression[String]) =
    new DefaultMultipleFindCheckBuilder[
      DseCheck,
      DseResponse,
      DseResponse,
      Any](ResponseExtender, PassThroughResponsePreparer) {
      def findExtractor(occurrence: Int) =
        columnName.map(new SingleColumnValueExtractor(_, occurrence))
      def findAllExtractor = columnName.map(new MultipleColumnValueExtractor(_))
      def countExtractor = columnName.map(new CountColumnValueExtractor(_))
    }
}
