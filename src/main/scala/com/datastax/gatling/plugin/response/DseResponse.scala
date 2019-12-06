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
  def coordinator(): Node = executionInfo.getCoordinator
  def speculativeExecutions(): Int = executionInfo.getSpeculativeExecutionCount
  def pagingState(): ByteBuffer = executionInfo.getPagingState
  def warnings(): List[String] = executionInfo.getWarnings.asScala.toList
  def successFullExecutionIndex(): Int = executionInfo.getSuccessfulExecutionIndex
  def schemaInAgreement(): Boolean = executionInfo.isSchemaInAgreement
}

class GraphResponse(graphResultSet: GraphResultSet, dseAttributes: DseGraphAttributes[_]) extends DseResponse with LazyLogging {

  override def executionInfo(): ExecutionInfo = graphResultSet.getExecutionInfo.asInstanceOf[ExecutionInfo]

  def getGraphResultSet: GraphResultSet = graphResultSet

  def getDseAttributes: DseGraphAttributes[_] = dseAttributes
}

class CqlResponse(cqlResultSet: AsyncResultSet, dseAttributes: DseCqlAttributes[_]) extends DseResponse with LazyLogging {

  override def executionInfo(): ExecutionInfo = cqlResultSet.getExecutionInfo

  def getCqlResultSet: AsyncResultSet = cqlResultSet

  def getDseAttributes: DseCqlAttributes[_] = dseAttributes
}
