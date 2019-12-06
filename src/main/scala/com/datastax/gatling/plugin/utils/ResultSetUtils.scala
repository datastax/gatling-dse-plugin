package com.datastax.gatling.plugin.utils

import com.datastax.dse.driver.api.core.graph.{GraphNode, GraphResultSet}
import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, ResultSet, Row}
import org.apache.tinkerpop.gremlin.process.traversal.Path
import org.apache.tinkerpop.gremlin.structure.{Edge, Property, Vertex, VertexProperty}

import scala.collection.JavaConverters._

object ResultSetUtils {

  def resultSetToSeq(resultSet:ResultSet): Seq[Row] = {
    iterableAsScalaIterable(resultSet).toSeq
  }

  def asyncResultSetToSeq(resultSet:AsyncResultSet): Seq[Row] = {
  }

  def graphResultSetToSeq(resultSet:GraphResultSet): Seq[GraphNode] = {
    iterableAsScalaIterable(resultSet).toSeq
  }

  private def buildFilterAndMapFn[T](filterFn: GraphNode => Boolean, mapFn: GraphNode => T)(resultSet: GraphResultSet, key:String):Seq[T] = {
    graphResultSetToSeq(resultSet )
      .map(graphNode => graphNode.getByKey(key))
      .filter(filterFn)
      .map(mapFn)
  }

  def getEdges: (GraphResultSet, String) => Seq[Edge] = buildFilterAndMapFn(_.isEdge, _.asEdge)

  def getVertexes: (GraphResultSet, String) => Seq[Vertex] = buildFilterAndMapFn(_.isVertex, _.asVertex)

  def getPaths: (GraphResultSet, String) => Seq[Path] = buildFilterAndMapFn(_.isPath, _.asPath)

  def getProperties: (GraphResultSet, String) => Seq[Property[_]] =
    buildFilterAndMapFn(_.isProperty, _.asProperty.asInstanceOf[Property[_]])

  def getVertexProperties: (GraphResultSet, String) => Seq[VertexProperty[_]] =
    buildFilterAndMapFn(_.isVertexProperty, _.asVertexProperty.asInstanceOf[VertexProperty[_]])
}
