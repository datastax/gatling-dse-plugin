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
import com.datastax.gatling.plugin.request.DseAttributes
import com.datastax.gatling.plugin.response.DseResponse
import io.gatling.commons.validation.{SuccessWrapper, Validation}
import io.gatling.core.check.extractor.{Extractor, SingleArity}
import io.gatling.core.check.{FindCheckBuilder, ValidatorCheckBuilder}
import io.gatling.core.session.{Expression, ExpressionSuccessWrapper}

import scala.collection.JavaConverters._


class DseResponseFindCheckBuilder[X](extractor: Expression[Extractor[DseResponse, X]])
    extends FindCheckBuilder[DseCheck, DseResponse, DseResponse, X] {

  def find: ValidatorCheckBuilder[DseCheck, DseResponse, DseResponse, X] = {
    ValidatorCheckBuilder(ResponseExtender, PassThroughResponsePreparer, extractor)
  }
}

object DseCheckBuilder {

  // Start Global Checks

  protected val ExecutionInfoExtractor = new Extractor[DseResponse, ExecutionInfo] with SingleArity {
    val name = "executionInfo"

    def apply(response: DseResponse): Validation[Option[ExecutionInfo]] = {
      response.resultSet match {
        case rs: ResultSet => Some(rs.getExecutionInfo).success
        case gr: GraphResultSet => Some(gr.getExecutionInfo).success
      }
    }
  }.expressionSuccess

  val ExecutionInfo = new DseResponseFindCheckBuilder[ExecutionInfo](ExecutionInfoExtractor)


  protected val QueriedHostExtractor = new Extractor[DseResponse, Host] with SingleArity {
    val name = "queriedHost"

    def apply(response: DseResponse): Validation[Option[Host]] = {
      response.resultSet match {
        case rs: ResultSet => Some(rs.getExecutionInfo.getQueriedHost).success
        case gr: GraphResultSet => Some(gr.getExecutionInfo.getQueriedHost).success
      }
    }
  }.expressionSuccess

  val QueriedHost = new DseResponseFindCheckBuilder[Host](QueriedHostExtractor)


  protected val AchievedConsistencyLevelExtractor = new Extractor[DseResponse, ConsistencyLevel] with SingleArity {
    val name = "achievedConsistencyLevel"

    def apply(response: DseResponse): Validation[Option[ConsistencyLevel]] = {
      response.resultSet match {
        case rs: ResultSet => Some(rs.getExecutionInfo.getAchievedConsistencyLevel).success
        case gr: GraphResultSet => Some(gr.getExecutionInfo.getAchievedConsistencyLevel).success
      }
    }
  }.expressionSuccess

  val AchievedConsistencyLevel = new DseResponseFindCheckBuilder[ConsistencyLevel](AchievedConsistencyLevelExtractor)


  protected val SpeculativeExecutionsExtractor = new Extractor[DseResponse, Int] with SingleArity {
    val name = "speculativeExecutions"

    def apply(response: DseResponse): Validation[Option[Int]] = {
      response.resultSet match {
        case rs: ResultSet => Some(rs.getExecutionInfo.getSpeculativeExecutions).success
        case gr: GraphResultSet => Some(gr.getExecutionInfo.getSpeculativeExecutions).success
      }
    }
  }.expressionSuccess

  val SpeculativeExecutions = new DseResponseFindCheckBuilder[Int](SpeculativeExecutionsExtractor)


  protected val PagingStateExtractor = new Extractor[DseResponse, PagingState] with SingleArity {
    val name = "pagingState"

    def apply(response: DseResponse): Validation[Option[PagingState]] = {
      response.resultSet match {
        case rs: ResultSet => Some(rs.getExecutionInfo.getPagingState).success
        case gr: GraphResultSet => Some(gr.getExecutionInfo.getPagingState).success
      }
    }
  }.expressionSuccess

  val PagingState = new DseResponseFindCheckBuilder[PagingState](PagingStateExtractor)


  protected val GetStatementExtractor = new Extractor[DseResponse, Statement] with SingleArity {
    val name = "pagingState"

    def apply(response: DseResponse): Validation[Option[Statement]] = {
      response.resultSet match {
        case rs: ResultSet => Some(rs.getExecutionInfo.getStatement).success
        case gr: GraphResultSet => Some(gr.getExecutionInfo.getStatement).success
      }
    }
  }.expressionSuccess

  val GetStatement = new DseResponseFindCheckBuilder[Statement](GetStatementExtractor)


  protected val TriedHostsExtractor = new Extractor[DseResponse, List[Host]] with SingleArity {
    val name = "triedHosts"

    def apply(response: DseResponse): Validation[Option[List[Host]]] = {
      response.resultSet match {
        case rs: ResultSet => Some(rs.getExecutionInfo.getTriedHosts.asScala.toList).success
        case gr: GraphResultSet => Some(gr.getExecutionInfo.getTriedHosts.asScala.toList).success
      }
    }
  }.expressionSuccess

  val TriedHosts = new DseResponseFindCheckBuilder[List[Host]](TriedHostsExtractor)


  protected val WarningsExtractor = new Extractor[DseResponse, List[String]] with SingleArity {
    val name = "warnings"

    def apply(response: DseResponse): Validation[Option[List[String]]] = {
      response.resultSet match {
        case rs: ResultSet => Some(rs.getExecutionInfo.getWarnings.asScala.toList).success
        case gr: GraphResultSet => Some(gr.getExecutionInfo.getWarnings.asScala.toList).success
      }
    }
  }.expressionSuccess

  val Warnings = new DseResponseFindCheckBuilder[List[String]](WarningsExtractor)


  protected val SuccessfulExecutionIndexExtractor = new Extractor[DseResponse, Int] with SingleArity {
    val name = "successfulExecutionIndex"

    def apply(response: DseResponse): Validation[Option[Int]] = {
      response.resultSet match {
        case rs: ResultSet => Some(rs.getExecutionInfo.getSuccessfulExecutionIndex).success
        case gr: GraphResultSet => Some(gr.getExecutionInfo.getSuccessfulExecutionIndex).success
      }
    }
  }.expressionSuccess

  val SuccessfulExecutionIndex = new DseResponseFindCheckBuilder[Int](SuccessfulExecutionIndexExtractor)


  protected val QueryTraceExtractor = new Extractor[DseResponse, QueryTrace] with SingleArity {
    val name = "queryTrace"

    def apply(response: DseResponse): Validation[Option[QueryTrace]] = {
      response.resultSet match {
        case rs: ResultSet => Some(rs.getExecutionInfo.getQueryTrace).success
        case gr: GraphResultSet => Some(gr.getExecutionInfo.getQueryTrace).success
      }
    }
  }.expressionSuccess

  val QueryTrace = new DseResponseFindCheckBuilder[QueryTrace](QueryTraceExtractor)


  protected val IncomingPayloadExtractor = new Extractor[DseResponse, Map[String, ByteBuffer]] with SingleArity {
    val name = "incomingPayload"

    def apply(response: DseResponse): Validation[Option[Map[String, ByteBuffer]]] = {
      response.resultSet match {
        case rs: ResultSet => Some(rs.getExecutionInfo.getIncomingPayload.asScala.toMap).success
        case gr: GraphResultSet => Some(gr.getExecutionInfo.getIncomingPayload.asScala.toMap).success
      }
    }
  }.expressionSuccess

  val IncomingPayload = new DseResponseFindCheckBuilder[Map[String, ByteBuffer]](IncomingPayloadExtractor)


  protected val SchemaInAgreementExtractor = new Extractor[DseResponse, Boolean] with SingleArity {
    val name = "schemaInAgreement"

    def apply(response: DseResponse): Validation[Option[Boolean]] = {
      response.resultSet match {
        case rs: ResultSet => Some(rs.getExecutionInfo.isSchemaInAgreement).success
        case gr: GraphResultSet => Some(gr.getExecutionInfo.isSchemaInAgreement).success
      }
    }
  }.expressionSuccess

  val SchemaInAgreement = new DseResponseFindCheckBuilder[Boolean](SchemaInAgreementExtractor)


  protected val RowCountExtractor = new Extractor[DseResponse, Int] with SingleArity {
    val name = "rowCount"

    def apply(response: DseResponse): Validation[Option[Int]] = {
      Some(response.getRowCount).success
    }
  }.expressionSuccess

  val RowCount = new DseResponseFindCheckBuilder[Int](RowCountExtractor)


  protected val AppliedExtractor = new Extractor[DseResponse, Boolean] with SingleArity {
    val name = "applied"

    def apply(response: DseResponse): Validation[Option[Boolean]] = {
      Some(response.isApplied).success
    }
  }.expressionSuccess

  val Applied = new DseResponseFindCheckBuilder[Boolean](AppliedExtractor)


  protected val ExhaustedExtractor = new Extractor[DseResponse, Boolean] with SingleArity {
    val name = "exhausted"

    def apply(response: DseResponse): Validation[Option[Boolean]] = {
      Some(response.isExhausted).success
    }
  }.expressionSuccess

  val Exhausted = new DseResponseFindCheckBuilder[Boolean](ExhaustedExtractor)


  protected val RequestAttributesExtractor = new Extractor[DseResponse, DseAttributes] with SingleArity {
    val name = "requestAttributes"

    def apply(response: DseResponse): Validation[Option[DseAttributes]] = {
      Some(response.getDseAttributes).success
    }
  }.expressionSuccess

  val RequestAttributes = new DseResponseFindCheckBuilder[DseAttributes](RequestAttributesExtractor)


  // Start CQL only Checks

  protected val ResultSetExtractor = new Extractor[DseResponse, ResultSet] with SingleArity {
    val name = "resultSet"

    def apply(response: DseResponse): Validation[Option[ResultSet]] = {
      Some(response.getCqlResultSet).success
    }
  }.expressionSuccess

  val ResultSet = new DseResponseFindCheckBuilder[ResultSet](ResultSetExtractor)


  protected val AllRowsExtractor = new Extractor[DseResponse, Seq[Row]] with SingleArity {
    val name = "allRows"

    def apply(response: DseResponse): Validation[Option[Seq[Row]]] = {
      Some(response.getAllRows).success
    }
  }.expressionSuccess

  val AllRows = new DseResponseFindCheckBuilder[Seq[Row]](AllRowsExtractor)


  protected val OneRowExtractor = new Extractor[DseResponse, Row] with SingleArity {
    val name = "oneRow"

    def apply(response: DseResponse): Validation[Option[Row]] = {
      Some(response.getOneRow).success
    }
  }.expressionSuccess

  val OneRow = new DseResponseFindCheckBuilder[Row](OneRowExtractor)

  // Start Graph only Checks

  protected val GraphResultSetExtractor = new Extractor[DseResponse, GraphResultSet] with SingleArity {
    val name = "graphResultSet"

    def apply(response: DseResponse): Validation[Option[GraphResultSet]] = {
      Some(response.getGraphResultSet).success
    }
  }.expressionSuccess

  val GraphResultSet = new DseResponseFindCheckBuilder[GraphResultSet](GraphResultSetExtractor)


  protected val AllNodesExtractor = new Extractor[DseResponse, Seq[GraphNode]] with SingleArity {
    val name = "allRows"

    def apply(response: DseResponse): Validation[Option[Seq[GraphNode]]] = {
      Some(response.getAllNodes).success
    }
  }.expressionSuccess

  val AllNodes = new DseResponseFindCheckBuilder[Seq[GraphNode]](AllNodesExtractor)


  protected val OneNodeExtractor = new Extractor[DseResponse, GraphNode] with SingleArity {
    val name = "oneNode"

    def apply(response: DseResponse): Validation[Option[GraphNode]] = {
      Some(response.getOneNode).success
    }
  }.expressionSuccess

  val OneNode = new DseResponseFindCheckBuilder[GraphNode](OneNodeExtractor)


  protected def EdgesExtractor(column: String) = new Extractor[DseResponse, Seq[Edge]] with SingleArity {
    val name = "edges"

    def apply(response: DseResponse): Validation[Option[Seq[Edge]]] = {
      Some(response.getEdges(column)).success
    }
  }.expressionSuccess

  def Edges(column: String) = new DseResponseFindCheckBuilder[Seq[Edge]](EdgesExtractor(column))


  protected def VertexesExtractor(column: String) = new Extractor[DseResponse, Seq[Vertex]] with SingleArity {
    val name = "vertexes"

    def apply(response: DseResponse): Validation[Option[Seq[Vertex]]] = {
      Some(response.getVertexes(column)).success
    }
  }.expressionSuccess

  def Vertexes(column: String) = new DseResponseFindCheckBuilder[Seq[Vertex]](VertexesExtractor(column))


  protected def PathsExtractor(column: String) = new Extractor[DseResponse, Seq[Path]] with SingleArity {
    val name = "paths"

    def apply(response: DseResponse): Validation[Option[Seq[Path]]] = {
      Some(response.getPaths(column)).success
    }
  }.expressionSuccess

  def Paths(column: String) = new DseResponseFindCheckBuilder[Seq[Path]](PathsExtractor(column))


  protected def PropertiesExtractor(column: String) = new Extractor[DseResponse, Seq[Property]] with SingleArity {
    val name = "properties"

    def apply(response: DseResponse): Validation[Some[Seq[Property]]] = {
      Some(response.getProperties(column)).success
    }
  }.expressionSuccess

  def Properties(column: String) = new DseResponseFindCheckBuilder[Seq[Property]](PropertiesExtractor(column))


  protected def VertexPropertiesExtractor(column: String) = new Extractor[DseResponse, Seq[Property]] with SingleArity {
    val name = "vertexProperties"

    def apply(response: DseResponse): Validation[Some[Seq[Property]]] = {
      Some(response.getVertexProperties(column)).success
    }
  }.expressionSuccess

  def VertexProperties(column: String) = new DseResponseFindCheckBuilder[Seq[Property]](VertexPropertiesExtractor(column))
}

