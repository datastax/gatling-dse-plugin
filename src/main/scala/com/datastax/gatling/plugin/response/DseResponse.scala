/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.response

import com.datastax.driver.core.{ResultSet, Row}
import com.datastax.driver.dse.graph._
import com.datastax.gatling.plugin.exceptions.DseCheckException
import com.typesafe.scalalogging.LazyLogging

class DseResponse(val resultSet: Any) extends LazyLogging {

  private var graphResultSet: Option[GraphResultSet] = None
  private var cqlResultSet: Option[ResultSet] = None

  resultSet match {
    case rs: ResultSet =>
      cqlResultSet = Some(rs)
    case gr: GraphResultSet =>
      graphResultSet = Some(gr)
  }

  private lazy val allCqlRows: Seq[Row] = collection.JavaConverters.asScalaBuffer(cqlResultSet.get.all())
  private lazy val allGraphNodes: Seq[GraphNode] = collection.JavaConverters.asScalaBuffer(graphResultSet.get.all())

  def getRowCount: Int =
    if (cqlResultSet.isDefined) allCqlRows.size
    else allGraphNodes.size

  def getAllRows: Seq[Row] = allCqlRows

  def getOneRow: Row =
    if (cqlResultSet.isEmpty) throw new DseCheckException("getOneRow requires the use of CQL Query")
    else cqlResultSet.get.one()

  def getAllNodes: Seq[GraphNode] = allGraphNodes

  def getOneNode: GraphNode =
    if (graphResultSet.isEmpty) throw new DseCheckException("getOneNode requires the use of Graph Query")
    else graphResultSet.get.one()

  def getCqlResultColumnValues(column: String): Seq[Any] = {
    if (cqlResultSet.isEmpty) throw new DseCheckException("getCqlResultColumnValues requires the use of CQL Query")

    if (allCqlRows.isEmpty || !allCqlRows.head.getColumnDefinitions.contains(column)) {
      return Seq.empty
    }

    allCqlRows.map(row => row.getObject(column))
  }

  def getGraphResultColumnValues(column: String): Seq[Any] = {
    if (graphResultSet.isEmpty) throw new DseCheckException("getGraphResultColumnValues requires the use of Graph Query")

    if (allGraphNodes.isEmpty) return Seq.empty

    allGraphNodes.map(node => if (node.get(column) != null) node.get(column))
  }
}
