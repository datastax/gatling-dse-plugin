/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.checks

import com.datastax.dse.driver.api.core.graph._
import com.datastax.gatling.plugin.response.GraphResponse
import com.datastax.gatling.plugin.utils.{GraphResultSetUtils, ResultSetUtils}
import io.gatling.commons.validation.{SuccessWrapper, Validation}
import io.gatling.core.check.extractor.{Extractor, SingleArity}
import io.gatling.core.check._
import io.gatling.core.session.{Expression, ExpressionSuccessWrapper, Session}
import org.apache.tinkerpop.gremlin.process.traversal.Path
import org.apache.tinkerpop.gremlin.structure.{Edge, Property, Vertex, VertexProperty}

import scala.collection.mutable


/**
  * This class serves as model for the Graph-specific checks.
  *
  * @param wrapped the underlying check
  */
case class DseGraphCheck(wrapped: Check[GraphResponse]) extends Check[GraphResponse] {
  override def check(response: GraphResponse, session: Session)(implicit cache: mutable.Map[Any, Any]): Validation[CheckResult] = {
    wrapped.check(response, session)
  }
}

class GraphCheckBuilder[X](extractor: Expression[Extractor[GraphResponse, X]])
  extends FindCheckBuilder[DseGraphCheck, GraphResponse, GraphResponse, X] {

  private val graphResponseExtender: Extender[DseGraphCheck, GraphResponse] =
    wrapped => DseGraphCheck(wrapped)

  def find: ValidatorCheckBuilder[DseGraphCheck, GraphResponse, GraphResponse, X] = {
    ValidatorCheckBuilder(graphResponseExtender, x => x.success, extractor)
  }
}

private class GraphResponseExtractor[X](val name: String,
                                        val extractor: GraphResponse => X)
  extends Extractor[GraphResponse, X] with SingleArity {

  override def apply(response: GraphResponse): Validation[Option[X]] = {
    Some(extractor.apply(response)).success
  }

  def toCheckBuilder: GraphCheckBuilder[X] = {
    new GraphCheckBuilder[X](this.expressionSuccess)
  }
}

object GraphChecks {
  val graphResultSet =
    new GraphResponseExtractor[AsyncGraphResultSet](
      "graphResultSet",
      r => r.getGraphResultSet)
      .toCheckBuilder

  val allNodes =
    new GraphResponseExtractor[Seq[GraphNode]](
      "allNodes",
      r => ResultSetUtils.asyncGraphResultSetToSeq(r.getGraphResultSet))
      .toCheckBuilder

  val oneNode =
    new GraphResponseExtractor[GraphNode](
      "oneNode",
      r => ResultSetUtils.asyncGraphResultSetToSeq(r.getGraphResultSet).head)
      .toCheckBuilder

  def edges(column: String):GraphCheckBuilder[Seq[Edge]] =
    new GraphResponseExtractor[Seq[Edge]](
      "edges",
      r => GraphResultSetUtils.edges(r.getGraphResultSet,column))
      .toCheckBuilder

  def vertexes(column: String):GraphCheckBuilder[Seq[Vertex]] =
    new GraphResponseExtractor[Seq[Vertex]](
      "vertices",
      r => GraphResultSetUtils.vertexes(r.getGraphResultSet, column))
      .toCheckBuilder

  def paths(column: String):GraphCheckBuilder[Seq[Path]] =
    new GraphResponseExtractor[Seq[Path]](
      "paths",
      r => GraphResultSetUtils.paths(r.getGraphResultSet, column))
      .toCheckBuilder

  def properties(column: String):GraphCheckBuilder[Seq[Property[_]]] =
    new GraphResponseExtractor[Seq[Property[_]]](
      "properties",
      r => GraphResultSetUtils.properties(r.getGraphResultSet, column))
      .toCheckBuilder

  def vertexProperties(column: String):GraphCheckBuilder[Seq[VertexProperty[_]]] =
    new GraphResponseExtractor[Seq[VertexProperty[_]]](
      "vertexProperties",
      r => GraphResultSetUtils.vertexProperties(r.getGraphResultSet, column))
      .toCheckBuilder
}