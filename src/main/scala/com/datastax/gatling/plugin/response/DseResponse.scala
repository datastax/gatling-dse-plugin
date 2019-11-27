/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.response

import java.nio.ByteBuffer

import com.datastax.gatling.plugin.model.{DseCqlAttributes, DseGraphAttributes}
import com.datastax.oss.driver.api.core.cql._
import com.datastax.dse.driver.api.core.graph._
import com.datastax.oss.driver.api.core.metadata.Node
import com.typesafe.scalalogging.LazyLogging
import org.apache.tinkerpop.gremlin.process.traversal.Path
import org.apache.tinkerpop.gremlin.structure.{Edge, Property, Vertex, VertexProperty}

import scala.collection.JavaConverters._

abstract class DseResponse {
  def executionInfo(): ExecutionInfo
  def rowCount(): Int
  def applied(): Boolean
  def exhausted(): Option[Boolean]
  def coordinator(): Node = executionInfo.getCoordinator
  def speculativeExecutions(): Int = executionInfo.getSpeculativeExecutionCount
  def pagingState(): ByteBuffer = executionInfo.getPagingState
  def warnings(): List[String] = executionInfo.getWarnings.asScala.toList
  def successFullExecutionIndex(): Int = executionInfo.getSuccessfulExecutionIndex
  def schemaInAgreement(): Boolean = executionInfo.isSchemaInAgreement
}

class GraphResponse(graphResultSet: GraphResultSet, dseAttributes: DseGraphAttributes[_]) extends DseResponse with LazyLogging {
  private lazy val allGraphNodes: Seq[GraphNode] = iterableAsScalaIterable(graphResultSet.all()).toSeq

  override def executionInfo(): ExecutionInfo = graphResultSet.getExecutionInfo.asInstanceOf[ExecutionInfo]

  override def applied(): Boolean = false // graph doesn't support LWTs so always return false
  override def exhausted(): Option[Boolean] = Option.empty

  /**
    * Get the number of all rows returned by the query.
    * Note: Calling this function fetches <b>all</b> rows from the result set!
    */
  override def rowCount(): Int = allGraphNodes.size

  def getGraphResultSet: GraphResultSet = graphResultSet

  def getAllNodes: Seq[GraphNode] = allGraphNodes

  def getOneNode: GraphNode = graphResultSet.one()

  def getGraphResultColumnValues(column: String): Iterable[GraphNode] =
    allGraphNodes.map(node => node.getByKey(column))
      .filter(_ != null)

  def buildFilterAndMapFn[T](filterFn: GraphNode => Boolean, mapFn: GraphNode => T): String => Seq[T] =
  { arg =>
    iterableAsScalaIterable[GraphNode](graphResultSet).toSeq
      .map(graphNode => graphNode.getByKey(arg))
      .filter(filterFn)
      .map(mapFn)
  }

  def getEdges: String => Seq[Edge] = buildFilterAndMapFn(_.isEdge, _.asEdge)

  def getVertexes: String => Seq[Vertex] = buildFilterAndMapFn(_.isVertex, _.asVertex)

  def getPaths: String => Seq[Path] = buildFilterAndMapFn(_.isPath, _.asPath)

  def getProperties: String => Seq[Property[_]] =
    buildFilterAndMapFn(_.isProperty, _.asProperty.asInstanceOf[Property[_]])

  def getVertexProperties: String => Seq[VertexProperty[_]] =
    buildFilterAndMapFn(_.isVertexProperty, _.asVertexProperty.asInstanceOf[VertexProperty[_]])

  def getDseAttributes: DseGraphAttributes[_] = dseAttributes
}

class CqlResponse(cqlResultSet: ResultSet, dseAttributes: DseCqlAttributes[_]) extends DseResponse with LazyLogging {
  private lazy val allCqlRows: Seq[Row] = iterableAsScalaIterable(cqlResultSet.all()).toSeq

  override def executionInfo(): ExecutionInfo = cqlResultSet.getExecutionInfo
  override def applied(): Boolean = cqlResultSet.wasApplied
  override def exhausted(): Option[Boolean] =
    Option(cqlResultSet.isFullyFetched && (cqlResultSet.getAvailableWithoutFetching == 0))

  /**
    * Get the number of all rows returned by the query.
    * Note: Calling this function fetches <b>all</b> rows from the result set!
    */
  def rowCount(): Int = allCqlRows.size

  /**
    * Get CQL ResultSet
    *
    * @return
    */
  def getCqlResultSet: ResultSet = cqlResultSet

  /**
    * Get all the rows in a Seq[Row] format
    *
    * @return
    */
  def getAllRowsSeq: Seq[Row] = allCqlRows

  def getOneRow: Row = cqlResultSet.one

  def getColumnValSeq(column: String): Seq[Any] = allCqlRows.map(_.getObject(column))

  def getDseAttributes: DseCqlAttributes[_] = dseAttributes
}
