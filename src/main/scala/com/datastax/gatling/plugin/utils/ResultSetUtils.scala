package com.datastax.gatling.plugin.utils

import com.datastax.dse.driver.api.core.graph.{AsyncGraphResultSet, GraphNode}
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

  def asyncGraphResultSetToSeq(resultSet:AsyncGraphResultSet): Seq[GraphNode] = {
  }

}

object GraphResultSetUtils {

  private def buildFilterAndMapFn[T](filterFn: GraphNode => Boolean, mapFn: GraphNode => T)(resultSet: AsyncGraphResultSet, key:String):Seq[T] = {
    ResultSetUtils.asyncGraphResultSetToSeq(resultSet )
      .map(graphNode => graphNode.getByKey(key))
      .filter(filterFn)
      .map(mapFn)
  }

  def edges: (AsyncGraphResultSet, String) => Seq[Edge] = buildFilterAndMapFn(_.isEdge, _.asEdge)

  def vertexes: (AsyncGraphResultSet, String) => Seq[Vertex] = buildFilterAndMapFn(_.isVertex, _.asVertex)

  def paths: (AsyncGraphResultSet, String) => Seq[Path] = buildFilterAndMapFn(_.isPath, _.asPath)

  def properties: (AsyncGraphResultSet, String) => Seq[Property[_]] =
    buildFilterAndMapFn(_.isProperty, _.asProperty.asInstanceOf[Property[_]])

  def vertexProperties: (AsyncGraphResultSet, String) => Seq[VertexProperty[_]] =
    buildFilterAndMapFn(_.isVertexProperty, _.asVertexProperty.asInstanceOf[VertexProperty[_]])
}