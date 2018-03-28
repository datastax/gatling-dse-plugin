package com.datastax.gatling.plugin.checks

import com.datastax.gatling.plugin.checks.DseCheckBuilders._
import com.datastax.gatling.plugin.response.DseResponse
import com.datastax.gatling.plugin.utils.{CountColumnValueExtractor, MultipleColumnValueExtractor, SingleColumnValueExtractor}
import io.gatling.core.check._
import io.gatling.core.session.{Expression, RichExpression}

trait DseCheckSupport {

  // start global checks
  lazy val exhausted = DseCheckBuilder.Exhausted
  lazy val applied = DseCheckBuilder.Applied
  lazy val rowCount = DseCheckBuilder.RowCount

  // execution info and subsets
  lazy val executionInfo = DseCheckBuilder.ExecutionInfo
  lazy val achievedCL = DseCheckBuilder.AchievedConsistencyLevel
  lazy val statement = DseCheckBuilder.GetStatement
  lazy val incomingPayload = DseCheckBuilder.IncomingPayload
  lazy val pagingState = DseCheckBuilder.PagingState
  lazy val queriedHost = DseCheckBuilder.QueriedHost
  lazy val schemaAgreement = DseCheckBuilder.SchemaInAgreement
  lazy val speculativeExecutions = DseCheckBuilder.SpeculativeExecutions
  lazy val successfulExecutionIndex = DseCheckBuilder.SuccessfulExecutionIndex
  lazy val triedHosts = DseCheckBuilder.TriedHosts
  lazy val warnings = DseCheckBuilder.Warnings
  lazy val dseAttributes = DseCheckBuilder.RequestAttributes


  // start CQL only checks
  lazy val resultSet = DseCheckBuilder.ResultSet
  lazy val allRows = DseCheckBuilder.AllRows
  lazy val oneRow = DseCheckBuilder.OneRow


  // start Graph only checks
  lazy val graphResultSet = DseCheckBuilder.GraphResultSet
  lazy val allNodes = DseCheckBuilder.AllNodes
  lazy val oneNode = DseCheckBuilder.OneNode
  def edges(columnName: String) = DseCheckBuilder.Edges(columnName)
  def vertexes(columnName: String) = DseCheckBuilder.Vertexes(columnName)
  def paths(columnName: String) = DseCheckBuilder.Paths(columnName)
  def properties(columnName: String) = DseCheckBuilder.Properties(columnName)
  def vertexProperties(columnName: String) = DseCheckBuilder.VertexProperties(columnName)


  /**
    * Get a column by name returned by the CQL statement.
    * Note that this statement implicitly fetches <b>all</b> rows from the result set!
    */
  def columnValue(columnName: Expression[String]) =
    new DefaultMultipleFindCheckBuilder[DseCheck, DseResponse, DseResponse, Any](ResponseExtender, PassThroughResponsePreparer) {
      def findExtractor(occurrence: Int) = columnName.map(new SingleColumnValueExtractor(_, occurrence))
      def findAllExtractor = columnName.map(new MultipleColumnValueExtractor(_))
      def countExtractor = columnName.map(new CountColumnValueExtractor(_))
    }
}

