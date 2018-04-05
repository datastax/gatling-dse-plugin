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
import io.gatling.commons.validation.{SuccessWrapper, Validation}
import io.gatling.core.check.extractor.{Extractor, SingleArity}
import io.gatling.core.check.{FindCheckBuilder, ValidatorCheckBuilder}
import io.gatling.core.session.ExpressionSuccessWrapper

import scala.collection.JavaConverters._

object DseChecks {

  //
  // Metadata related checks
  //
  val executionInfo = new DseCheckBuilder[ExecutionInfo](
    new GenericResultSetExtractor[ExecutionInfo](
      "executionInfo",
      r => r.getExecutionInfo,
      gr => gr.getExecutionInfo))

  val queriedHost = new DseCheckBuilder[Host](
    new GenericResultSetExtractor[Host](
      "queriedHost",
      r => r.getExecutionInfo.getQueriedHost,
      gr => gr.getExecutionInfo.getQueriedHost))

  val achievedConsistencyLevel = new DseCheckBuilder[ConsistencyLevel](
    new GenericResultSetExtractor[ConsistencyLevel](
      "achievedConsistencyLevel",
      r => r.getExecutionInfo.getAchievedConsistencyLevel,
      gr => gr.getExecutionInfo.getAchievedConsistencyLevel))

  val speculativeExecutions = new DseCheckBuilder[Int](
    new GenericResultSetExtractor[Int](
      "speculativeExecutions",
      r => r.getExecutionInfo.getSpeculativeExecutions,
      gr => gr.getExecutionInfo.getSpeculativeExecutions))

  val pagingState = new DseCheckBuilder[PagingState](
    new GenericResultSetExtractor[PagingState](
      "pagingState",
      r => r.getExecutionInfo.getPagingState,
      gr => gr.getExecutionInfo.getPagingState))

  val statement = new DseCheckBuilder[Statement](
    new GenericResultSetExtractor[Statement](
      "statement",
      r => r.getExecutionInfo.getStatement,
      gr => gr.getExecutionInfo.getStatement))

  val triedHosts = new DseCheckBuilder[List[Host]](
    new GenericResultSetExtractor[List[Host]](
      "triedHosts",
      r => r.getExecutionInfo.getTriedHosts.asScala.toList,
      gr => gr.getExecutionInfo.getTriedHosts.asScala.toList))

  val warnings = new DseCheckBuilder[List[String]](
    new GenericResultSetExtractor[List[String]](
      "warnings",
      r => r.getExecutionInfo.getWarnings.asScala.toList,
      gr => gr.getExecutionInfo.getWarnings.asScala.toList))

  val successfulExecutionIndex = new DseCheckBuilder[Int](
    new GenericResultSetExtractor[Int](
      "successfulExecutionIndex",
      r => r.getExecutionInfo.getSuccessfulExecutionIndex,
      gr => gr.getExecutionInfo.getSuccessfulExecutionIndex))

  val queryTrace = new DseCheckBuilder[QueryTrace](
    new GenericResultSetExtractor[QueryTrace](
      "queryTrace",
      r => r.getExecutionInfo.getQueryTrace,
      gr => gr.getExecutionInfo.getQueryTrace))

  val incomingPayload = new DseCheckBuilder[Map[String, ByteBuffer]](
    new GenericResultSetExtractor[Map[String, ByteBuffer]](
      "incomingPayload",
      r => r.getExecutionInfo.getIncomingPayload.asScala.toMap,
      gr => gr.getExecutionInfo.getIncomingPayload.asScala.toMap))

  val schemaInAgreement = new DseCheckBuilder[Boolean](
    new GenericResultSetExtractor[Boolean](
      "schemaInAgreement",
      r => r.getExecutionInfo.isSchemaInAgreement,
      gr => gr.getExecutionInfo.isSchemaInAgreement))

  val applied = new DseCheckBuilder[Boolean](
    new GenericResultSetExtractor[Boolean](
      "applied",
      r => r.wasApplied(),
      gr => false)) // Graph does not support LWT

  val exhausted = new DseCheckBuilder[Boolean](
    new GenericResultSetExtractor[Boolean](
      "exhausted",
      r => r.isExhausted,
      gr => gr.isExhausted))

  //
  // CQL/Graph data dependent checks
  //
  // Calling all() empties the ResultSet once the data is returned.
  // A memoised data structure has to be used to allow multiple checks on all(),
  // count() and one() to be usable
  //
  val rowCount = new DseCheckBuilder[Int](
    new DseResponseExtractor[Int]("rowCount", r => r.getRowCount))

  val allRows = new DseCheckBuilder[Seq[Row]](
    new DseResponseExtractor[Seq[Row]]("allRows", r => r.getAllRows))

  val oneRow = new DseCheckBuilder[Row](
    new DseResponseExtractor[Row]("oneRows", r => r.getOneRow))

  val allNodes = new DseCheckBuilder[Seq[GraphNode]](
    new DseResponseExtractor[Seq[GraphNode]]("allNodes", r => r.getAllNodes))

  val oneNode = new DseCheckBuilder[GraphNode](
    new DseResponseExtractor[GraphNode]("oneNode", r => r.getOneNode))

  //
  // CQL/Graph generic checks
  //
  val resultSet = new DseCheckBuilder[ResultSet](
    new CqlResultSetExtractor[ResultSet]("resultSet", r => r))

  val graphResultSet = new DseCheckBuilder[GraphResultSet](
    new GraphResultSetExtractor[GraphResultSet]("graphResultSet", r => r))

  def edges(name: String): DseCheckBuilder[Seq[Edge]] =
    new DseCheckBuilder[Seq[Edge]](
      new GraphResultSetExtractor[Seq[Edge]](
        "edges",
        r =>
          r.all()
            .asScala
            .filter(node => node.isEdge)
            .map(node => node.get(name).asEdge())))

  def vertices(name: String): DseCheckBuilder[Seq[Vertex]] =
    new DseCheckBuilder[Seq[Vertex]](
      new GraphResultSetExtractor[Seq[Vertex]](
        "vertices",
        r =>
          r.all()
            .asScala
            .filter(node => node.isVertex)
            .map(node => node.get(name).asVertex())))

  def paths(name: String): DseCheckBuilder[Seq[Path]] =
    new DseCheckBuilder[Seq[Path]](
      new GraphResultSetExtractor[Seq[Path]](
        "paths",
        r => r.all().asScala.map(node => node.get(name).asPath())))

  def properties(name: String): DseCheckBuilder[Seq[Property]] =
    new DseCheckBuilder[Seq[Property]](
      new GraphResultSetExtractor[Seq[Property]](
        "properties",
        r => r.all().asScala.map(node => node.get(name).asProperty())))

  def vertexProperties(name: String): DseCheckBuilder[Seq[VertexProperty]] =
    new DseCheckBuilder[Seq[VertexProperty]](
      new GraphResultSetExtractor[Seq[VertexProperty]](
        "vertexProperties",
        r => r.all().asScala.map(node => node.get(name).asVertexProperty())))
}

/**
  * Generic class that can extract data out of DSE result sets.
  *
  * The API of [[ResultSet]] and [[GraphResultSet]] is similar, but the classes
  * do not share any common ancestor.  Therefore, two lambdas must be passed in
  * order to extract information from one or the other, depending on the type of
  * query that is issued.  In the vas majority of cases, they will be identical,
  * except for the type they can be apply to.
  *
  * @param name                    the name of the extractor
  * @param resultSetExtractor      the lambda that should be applied on
  *                                ResultSets
  * @param graphResultSetExtractor the lambda that should be applied on
  *                                GraphResultSets
  * @tparam X the type of data that is extracted
  */
private[checks] class GenericResultSetExtractor[X](
    val name: String,
    val resultSetExtractor: ResultSet => X,
    val graphResultSetExtractor: GraphResultSet => X)
    extends Extractor[DseResponse, X]
    with SingleArity {
  override def apply(response: DseResponse): Validation[Option[X]] =
    response.resultSet match {
      case r: ResultSet       => Some(resultSetExtractor.apply(r)).success
      case gr: GraphResultSet => Some(graphResultSetExtractor.apply(gr)).success
    }
}

private[checks] class CqlResultSetExtractor[X](
    val name: String,
    val extractor: ResultSet => X)
    extends Extractor[DseResponse, X]
    with SingleArity {
  override def apply(response: DseResponse): Validation[Option[X]] =
    response.resultSet match {
      case r: ResultSet => Some(extractor.apply(r)).success
      case gr: GraphResultSet =>
        throw new UnsupportedOperationException(
          s"Operation $name is not supported on Graph result sets")
    }
}

private[checks] class GraphResultSetExtractor[X](
    val name: String,
    val extractor: GraphResultSet => X)
    extends Extractor[DseResponse, X]
    with SingleArity {
  override def apply(response: DseResponse): Validation[Option[X]] =
    response.resultSet match {
      case r: ResultSet =>
        throw new UnsupportedOperationException(
          s"Operation $name is not supported on CQL result sets")
      case gr: GraphResultSet => Some(extractor.apply(gr)).success
    }
}

private[checks] class DseResponseExtractor[X](
    val name: String,
    val extractor: DseResponse => X)
    extends Extractor[DseResponse, X]
    with SingleArity {
  override def apply(response: DseResponse): Validation[Option[X]] =
    Some(extractor.apply(response)).success
}

private[checks] class DseCheckBuilder[X](extractor: Extractor[DseResponse, X])
    extends FindCheckBuilder[DseCheck, DseResponse, DseResponse, X] {
  def find: ValidatorCheckBuilder[DseCheck, DseResponse, DseResponse, X] =
    ValidatorCheckBuilder(
      ResponseExtender,
      PassThroughResponsePreparer,
      extractor.expressionSuccess)
}
