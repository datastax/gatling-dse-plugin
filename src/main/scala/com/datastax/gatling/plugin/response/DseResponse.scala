package com.datastax.gatling.plugin.response

import com.datastax.driver.core.{ResultSet, Row}
import com.datastax.driver.dse.graph._
import com.datastax.gatling.plugin.exceptions.DseCheckException
import com.datastax.gatling.plugin.request.DseAttributes
import com.typesafe.scalalogging.LazyLogging

import scala.util.Try

case class DseResponseBase(resultSet: Any)

class DseResponse(dseResultSet: Any, dseAttributes: DseAttributes) extends DseResponseBase(dseResultSet) with LazyLogging {

  private var graphResultSet: Option[GraphResultSet] = None
  private var cqlResultSet: Option[ResultSet] = None

  dseResultSet match {
    case rs: ResultSet =>
      cqlResultSet = Some(rs)
    case gr: GraphResultSet =>
      graphResultSet = Some(gr)
  }

  private lazy val allCqlRows: Seq[Row] = collection.JavaConverters.asScalaBuffer(cqlResultSet.get.all())
  private lazy val allGraphNodes: Seq[GraphNode] = collection.JavaConverters.asScalaBuffer(graphResultSet.get.all())


  /**
    * Was the LWT applied
    *
    * @return
    */
  def isApplied: Boolean = {
    if (graphResultSet.isDefined) {
      return false // graph doesn't support LWTs so always return false
    }
    cqlResultSet.get.wasApplied()
  }


  /**
    * Is the current resultSet completed in fetching
    *
    * @return
    */
  def isExhausted: Boolean = {
    if (cqlResultSet.isDefined) {
      cqlResultSet.get.isExhausted
    } else {
      graphResultSet.get.isExhausted
    }
  }

  /**
    * Get the number of all rows returned by the query.
    * Note: Calling this function fetches <b>all</b> rows from the result set!
    */
  def getRowCount: Int = {
    if (cqlResultSet.isDefined) {
      allCqlRows.size
    } else {
      allGraphNodes.size
    }
  }

  /**
    * Get CQL ResultSet
    *
    * @return
    */
  def getCqlResultSet: ResultSet = {
    if (cqlResultSet.isEmpty) throw new DseCheckException("getCqlResultSet requires the use of CQL Query")
    cqlResultSet.get
  }


  /**
    * Get all the rows in a Seq[Row] format
    *
    * @return
    */
  def getAllRows: Seq[Row] = allCqlRows


  def getOneRow: Row = {
    if (cqlResultSet.isEmpty) throw new DseCheckException("getOneRow requires the use of CQL Query")

    cqlResultSet.get.one()
  }


  def getCqlResultColumnValues(column: String): Seq[Any] = {
    if (cqlResultSet.isEmpty) throw new DseCheckException("getCqlResultColumnValues requires the use of CQL Query")

    if (allCqlRows.isEmpty || !allCqlRows.head.getColumnDefinitions.contains(column)) {
      return Seq.empty
    }

    allCqlRows.map(row => row.getObject(column))
  }


  def getGraphResultSet: GraphResultSet = {
    if (graphResultSet.isEmpty) throw new DseCheckException("getGraphResultSet requires the use of CQL Query")
    graphResultSet.get
  }

  def getAllNodes: Seq[GraphNode] = allGraphNodes


  def getOneNode: GraphNode = {
    if (graphResultSet.isEmpty) throw new DseCheckException("getOneNode requires the use of Graph Query")

    graphResultSet.get.one()
  }


  def getGraphResultColumnValues(column: String): Seq[Any] = {
    if (graphResultSet.isEmpty) throw new DseCheckException("getGraphResultColumnValues requires the use of Graph Query")

    if (allGraphNodes.isEmpty) return Seq.empty

    allGraphNodes.map(node => if (node.get(column) != null) node.get(column))
  }


  def getEdges(name: String): Seq[Edge] = {
    if (graphResultSet.isEmpty) throw new DseCheckException("getEdges requires the use of a Graph Query")

    val columnValues = collection.mutable.Buffer[Edge]()
    graphResultSet.get.forEach { n =>
      Try(
        if (n.get(name).isEdge) {
          columnValues.append(n.get(name).asEdge())
        }
      )
    }
    columnValues
  }


  def getVertexes(name: String): Seq[Vertex] = {
    if (graphResultSet.isEmpty) throw new DseCheckException("getVertexes requires the use of a Graph Query")

    val columnValues = collection.mutable.Buffer[Vertex]()
    graphResultSet.get.forEach { n => Try(if (n.get(name).isVertex) columnValues.append(n.get(name).asVertex())) }
    columnValues
  }


  def getPaths(name: String): Seq[Path] = {
    if (graphResultSet.isEmpty) throw new DseCheckException("getPaths requires the use of a Graph Query")

    val columnValues = collection.mutable.Buffer[Path]()
    graphResultSet.get.forEach { n => Try(columnValues.append(n.get(name).asPath())) }
    columnValues
  }


  def getProperties(name: String): Seq[Property] = {
    if (graphResultSet.isEmpty) throw new DseCheckException("getProperties requires the use of a Graph Query")

    val columnValues = collection.mutable.Buffer[Property]()
    graphResultSet.get.forEach { n => Try(columnValues.append(n.get(name).asProperty())) }
    columnValues
  }


  def getVertexProperties(name: String): Seq[VertexProperty] = {
    if (graphResultSet.isEmpty) throw new DseCheckException("getVertexProperties requires the use of a Graph Query")

    val columnValues = collection.mutable.Buffer[VertexProperty]()
    graphResultSet.get.forEach { n => Try(columnValues.append(n.get(name).asVertexProperty())) }
    columnValues
  }

  def getDseAttributes = dseAttributes
}
