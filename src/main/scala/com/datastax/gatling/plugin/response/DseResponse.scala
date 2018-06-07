/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.response

import com.datastax.driver.core._
import com.datastax.driver.dse.graph._
import com.datastax.gatling.plugin.request.{DseCqlAttributes, DseGraphAttributes}
import com.typesafe.scalalogging.LazyLogging
import scala.collection.JavaConverters._

import scala.util.Try

abstract class DseResponse {
  def executionInfo(): ExecutionInfo
  def rowCount(): Int
  def applied(): Boolean
  def exhausted(): Boolean
  def queriedHost(): Host = executionInfo().getQueriedHost
  def achievedConsistencyLevel(): ConsistencyLevel = executionInfo().getAchievedConsistencyLevel
  def speculativeExecutions(): Int = executionInfo().getSpeculativeExecutions
  def pagingState(): PagingState = executionInfo().getPagingState
  def triedHosts(): List[Host] = executionInfo().getTriedHosts.asScala.toList
  def warnings(): List[String] = executionInfo().getWarnings.asScala.toList
  def successFullExecutionIndex(): Int = executionInfo().getSuccessfulExecutionIndex
  def schemaInAgreement(): Boolean = executionInfo().isSchemaInAgreement
}


class GraphResponse(graphResultSet: GraphResultSet, dseAttributes: DseGraphAttributes) extends DseResponse with LazyLogging {
  private lazy val allGraphNodes: Seq[GraphNode] = collection.JavaConverters.asScalaBuffer(graphResultSet.all())

  override def executionInfo(): ExecutionInfo = graphResultSet.getExecutionInfo()
  override def applied(): Boolean = false // graph doesn't support LWTs so always return false
  override def exhausted(): Boolean = graphResultSet.isExhausted()

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

    allGraphNodes.map(node => if (node.get(column) != null) node.get(column))
  }


  def getEdges(name: String): Seq[Edge] = {
    val columnValues = collection.mutable.Buffer[Edge]()
    graphResultSet.forEach { n =>
      Try(
        if (n.get(name).isEdge) {
          columnValues.append(n.get(name).asEdge())
        }
      )
    }
    columnValues
  }


  def getVertexes(name: String): Seq[Vertex] = {
    val columnValues = collection.mutable.Buffer[Vertex]()
    graphResultSet.forEach { n => Try(if (n.get(name).isVertex) columnValues.append(n.get(name).asVertex())) }
    columnValues
  }


  def getPaths(name: String): Seq[Path] = {
    val columnValues = collection.mutable.Buffer[Path]()
    graphResultSet.forEach { n => Try(columnValues.append(n.get(name).asPath())) }
    columnValues
  }


  def getProperties(name: String): Seq[Property] = {
    val columnValues = collection.mutable.Buffer[Property]()
    graphResultSet.forEach { n => Try(columnValues.append(n.get(name).asProperty())) }
    columnValues
  }


  def getVertexProperties(name: String): Seq[VertexProperty] = {
    val columnValues = collection.mutable.Buffer[VertexProperty]()
    graphResultSet.forEach { n => Try(columnValues.append(n.get(name).asVertexProperty())) }
    columnValues
  }

  def getDseAttributes: DseGraphAttributes = dseAttributes
}

class CqlResponse(cqlResultSet: ResultSet, dseAttributes: DseCqlAttributes) extends DseResponse with LazyLogging {
  private lazy val allCqlRows: Seq[Row] = collection.JavaConverters.asScalaBuffer(cqlResultSet.all())

  override def executionInfo(): ExecutionInfo = cqlResultSet.getExecutionInfo()
  override def applied(): Boolean = cqlResultSet.wasApplied()
  override def exhausted(): Boolean = cqlResultSet.isExhausted()

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
