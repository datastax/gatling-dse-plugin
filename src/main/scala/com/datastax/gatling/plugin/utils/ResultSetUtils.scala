package com.datastax.gatling.plugin.utils

import java.util.concurrent.CompletionStage

import com.datastax.dse.driver.api.core.graph.{ AsyncGraphResultSet, GraphNode }
import com.datastax.oss.driver.api.core.cql.{ AsyncResultSet, Row }
import org.apache.tinkerpop.gremlin.process.traversal.Path
import org.apache.tinkerpop.gremlin.structure.{ Edge, Property, Vertex, VertexProperty }

import collection.JavaConverters._

object ResultSetUtils {

  def asyncResultSetToIterator(resultSet: AsyncResultSet): Iterator[Row] =
    new IteratorFromAsyncResultSet(new CqlAsyncRS(resultSet))

  def asyncGraphResultSetToIterator(resultSet: AsyncGraphResultSet): Iterator[GraphNode] =
    new IteratorFromAsyncResultSet(new GraphAsyncRS(resultSet))
}

object GraphResultSetUtils {

  private def buildFilterAndMapFn[T](filterFn: GraphNode => Boolean, mapFn: GraphNode => T)(resultSet: AsyncGraphResultSet, key: String): Iterator[T] =
    ResultSetUtils
      .asyncGraphResultSetToIterator(resultSet)
      .map(graphNode => graphNode.getByKey(key))
      .filter(filterFn)
      .map(mapFn)

  def edges: (AsyncGraphResultSet, String) => Iterator[Edge] = buildFilterAndMapFn(_.isEdge, _.asEdge)

  def vertexes: (AsyncGraphResultSet, String) => Iterator[Vertex] = buildFilterAndMapFn(_.isVertex, _.asVertex)

  def paths: (AsyncGraphResultSet, String) => Iterator[Path] = buildFilterAndMapFn(_.isPath, _.asPath)

  def properties: (AsyncGraphResultSet, String) => Iterator[Property[_]] =
    buildFilterAndMapFn(_.isProperty, _.asProperty.asInstanceOf[Property[_]])

  def vertexProperties: (AsyncGraphResultSet, String) => Iterator[VertexProperty[_]] =
    buildFilterAndMapFn(_.isVertexProperty, _.asVertexProperty.asInstanceOf[VertexProperty[_]])
}

trait AsyncRS[T] {
  def hasMorePages: Boolean
  def currentPage: Iterable[T]
  def fetchNextPage: CompletionStage[AsyncRS[T]]
}

class CqlAsyncRS(rs: AsyncResultSet) extends AsyncRS[Row] {
  override def hasMorePages: Boolean                        = rs.hasMorePages
  override def currentPage: Iterable[Row]                   = rs.currentPage.asScala
  override def fetchNextPage: CompletionStage[AsyncRS[Row]] = rs.fetchNextPage.thenApplyAsync(new CqlAsyncRS(_))
}

class GraphAsyncRS(rs: AsyncGraphResultSet) extends AsyncRS[GraphNode] {
  override def hasMorePages: Boolean                              = rs.hasMorePages
  override def currentPage: Iterable[GraphNode]                   = rs.currentPage.asScala
  override def fetchNextPage: CompletionStage[AsyncRS[GraphNode]] = rs.fetchNextPage.thenApplyAsync(new GraphAsyncRS(_))
}

/* Note that this iterator isn't thread-safe */
class IteratorFromAsyncResultSet[T](rs: AsyncRS[T]) extends Iterator[T] {
  var working                   = rs
  var iter                      = rs.currentPage.iterator
  override def hasNext: Boolean = iter.hasNext || working.hasMorePages
  override def next: T = {
    if (!iter.hasNext && working.hasMorePages) {
      working = working.fetchNextPage.toCompletableFuture.get
      iter = working.currentPage.iterator
    }
    iter.next
  }
}
