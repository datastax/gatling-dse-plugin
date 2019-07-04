/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.response

import com.datastax.oss.driver.api.core.cql._
import com.datastax.oss.driver.api.core.metadata._
import com.datastax.dse.driver.api.core.graph._
import com.datastax.gatling.plugin.model.{DseCqlAttributes, DseGraphAttributes}
import com.typesafe.scalalogging.LazyLogging

import scala.util.Try
import java.nio.ByteBuffer
import org.apache.tinkerpop.gremlin.structure._
import org.apache.tinkerpop.gremlin.process.traversal.Path
import java.util.stream.Collectors

abstract class DseResponse[E] {
  def executionInfo(): E
  def rowCount(): Int
  def applied(): Boolean
  def queriedHost(): Node = executionInfo().getCoordinator()
  def speculativeExecutions(): Int = executionInfo().getSpeculativeExecutionCount
  def pagingState(): ByteBuffer = executionInfo().getPagingState
  def triedHosts(): List[Node] = executionInfo().getErrors.stream.map(_.getKey).collect(Collectors.toList[Node]).asScala.toList
  def warnings(): List[String] = executionInfo().getWarnings.asScala.toList
  def successFullExecutionIndex(): Int = executionInfo().getSuccessfulExecutionIndex
  def schemaInAgreement(): Boolean = executionInfo().isSchemaInAgreement
}


class GraphResponse(graphResultSet: GraphResultSet, dseAttributes: DseGraphAttributes) extends DseResponse with LazyLogging {
  private lazy val allGraphNodes: Seq[GraphNode] = collection.JavaConverters.asScalaBuffer(graphResultSet.all())

  def graphExecutionInfo(): GraphExecutionInfo = graphResultSet.getExecutionInfo()
  override def executionInfo(): ExecutionInfo = null
  override def applied(): Boolean = false // graph doesn't support LWTs so always return false
  //override def exhausted(): Boolean = isFullyFetched()

  /**
    * Get the number of all rows returned by the query.
    * Note: Calling this function fetches <b>all</b> rows from the result set!
    */
  override def rowCount(): Int = allGraphNodes.size

  def getGraphResultSet: GraphResultSet = {
    graphResultSet
  }

  def getAllNodes: Seq[GraphNode] = allGraphNodes

  def getOneNode: GraphNode = {
    graphResultSet.one()
  }


  def getGraphResultColumnValues(column: String): Seq[Any] = {
    if (allGraphNodes.isEmpty) return Seq.empty

    allGraphNodes.map(node => if (node.getByKey(column) != null) node.getByKey(column))
  }


  def getEdges(name: String): Seq[Edge] = {
    val columnValues = collection.mutable.Buffer[Edge]()
    graphResultSet.forEach { n =>
      Try(
        if (n.getByKey(name).isEdge) {
          columnValues.append(n.getByKey(name).asEdge())
        }
      )
    }
    columnValues
  }


  def getVertexes(name: String): Seq[Vertex] = {
    val columnValues = collection.mutable.Buffer[Vertex]()
    graphResultSet.forEach { n => Try(if (n.getByKey(name).isVertex) columnValues.append(n.getByKey(name).asVertex())) }
    columnValues
  }


  def getPaths(name: String): Seq[Path] = {
    val columnValues = collection.mutable.Buffer[Path]()
    graphResultSet.forEach { n => Try(columnValues.append(n.getByKey(name).asPath())) }
    columnValues
  }


  def getProperties(name: String): Seq[Property[String]] = {
    val columnValues = collection.mutable.Buffer[Property[String]]()
    graphResultSet.forEach { n => Try(columnValues.append(n.getByKey(name).asProperty())) }
    columnValues
  }


  def getVertexProperties(name: String): Seq[VertexProperty[String]] = {
    val columnValues = collection.mutable.Buffer[VertexProperty[String]]()
    graphResultSet.forEach { n => Try(columnValues.append(n.getByKey(name).asVertexProperty())) }
    columnValues
  }

  def getDseAttributes: DseGraphAttributes = dseAttributes
}

class CqlResponse[T](cqlResultSet: ResultSet, dseAttributes: DseCqlAttributes[T]) extends DseResponse with LazyLogging {
  private lazy val allCqlRows: Seq[Row] = collection.JavaConverters.asScalaBuffer(cqlResultSet.all())

  override def executionInfo(): ExecutionInfo = cqlResultSet.getExecutionInfo()
  override def applied(): Boolean = cqlResultSet.wasApplied()

  /**
    * Get the number of all rows returned by the query.
    * Note: Calling this function fetches <b>all</b> rows from the result set!
    */
  def rowCount: Int = allCqlRows.size

  /**
    * Get CQL ResultSet
    *
    * @return
    */
  def getCqlResultSet: ResultSet = {
    cqlResultSet
  }


  /**
    * Get all the rows in a Seq[Row] format
    *
    * @return
    */
  def getAllRows: Seq[Row] = allCqlRows


  def getOneRow: Row = {
    cqlResultSet.one()
  }


  def getCqlResultColumnValues(column: String): Seq[Any] = {
    if (allCqlRows.isEmpty || !allCqlRows.head.getColumnDefinitions.contains(column)) {
      return Seq.empty
    }

    allCqlRows.map(row => row.getObject(column))
  }

  def getDseAttributes: DseCqlAttributes = dseAttributes
}
