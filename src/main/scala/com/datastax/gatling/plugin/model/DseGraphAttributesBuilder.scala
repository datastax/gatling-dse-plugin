/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.model

import java.time.Duration

import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.dse.driver.api.core.graph.{GraphNode, GraphStatement}
import com.datastax.gatling.plugin.checks.{DseGraphCheck, GenericCheck}
import com.datastax.gatling.plugin.request.GraphRequestActionBuilder
import com.datastax.oss.driver.api.core.metadata.Node
import com.datastax.oss.driver.shaded.guava.common.base.Function

/**
  * Request Builder for Graph Requests
  *
  * @param attr Addition Attributes
  */
case class DseGraphAttributesBuilder[T <: GraphStatement[T]](attr: DseGraphAttributes[T]) {
  /**
    * Builds to final action to run
    *
    * @return
    */
  def build(): GraphRequestActionBuilder[T] = new GraphRequestActionBuilder(attr)

  /**
    * Set Consistency Level
    *
    * @param level ConsistencyLevel
    * @return
    */
  def withConsistencyLevel(level: ConsistencyLevel):DseGraphAttributesBuilder[T] =
    DseGraphAttributesBuilder(attr.copy(cl = Some(level)))

  /**
    * Set query to be idempotent i.e. run only once
    *
    * @return
    */
  def withIdempotency():DseGraphAttributesBuilder[T] =
    DseGraphAttributesBuilder(attr.copy(idempotent = Some(true)))

  /**
    * Sets the graph name to use
    *
    * @param name Graph name
    * @return
    */
  def withName(name: String):DseGraphAttributesBuilder[T] =
    DseGraphAttributesBuilder(attr.copy(graphName = Some(name)))

  /**
    * Set the node that should handle this query
    * @param node Node
    * @return
    */
  def withNode(node: Node):DseGraphAttributesBuilder[T] =
    DseGraphAttributesBuilder(attr.copy(node = Some(node)))

  /**
    * Define [[ConsistencyLevel]] to be used for read queries
    *
    * @param readCL Consistency Level to use
    * @return
    */
  def withReadConsistency(readCL: ConsistencyLevel):DseGraphAttributesBuilder[T] =
    DseGraphAttributesBuilder(attr.copy(readCL = Some(readCL)))

  /**
    * Set the sub-protocol
    *
    * @param subProtocol the sub-protocol to use
    * @return
    */
  def withSubProtocol(subProtocol: String):DseGraphAttributesBuilder[T] =
    DseGraphAttributesBuilder(attr.copy(subProtocol = Some(subProtocol)))

  /**
    * Set the timeout
    *
    * @param timeout the timeout to use
    * @return
    */
  def withTimeout(timeout: Duration):DseGraphAttributesBuilder[T] =
    DseGraphAttributesBuilder(attr.copy(timeout = Some(timeout)))

  /**
    * Set the timestamp
    *
    * @param timestamp the timestamp to use
    * @return
    */
  def withTimestamp(timestamp: Long):DseGraphAttributesBuilder[T] =
    DseGraphAttributesBuilder(attr.copy(timestamp = Some(timestamp)))

  /**
    * Set the sub-protocol
    *
    * @param traversalSource the traversal source to use
    * @return
    */
  def withTraversalSource(traversalSource: String):DseGraphAttributesBuilder[T] =
    DseGraphAttributesBuilder(attr.copy(traversalSource = Some(traversalSource)))

  /**
    * Define [[ConsistencyLevel]] to be used for write queries
    *
    * @param writeCL Consistency Level to use
    * @return
    */
  def withWriteConsistency(writeCL: ConsistencyLevel):DseGraphAttributesBuilder[T] =
    DseGraphAttributesBuilder(attr.copy(writeCL = Some(writeCL)))

  /**
    * Backwards compatibility to set consistencyLevel
    *
    * @see [[DseGraphAttributesBuilder.withConsistencyLevel]]
    * @param level Consistency Level to use
    * @return
    */
  @deprecated("use withConsistencyLevel() instead, will be removed in future version")
  def consistencyLevel(level: ConsistencyLevel):DseGraphAttributesBuilder[T] = withConsistencyLevel(level)

  def check(check: DseGraphCheck):DseGraphAttributesBuilder[T] =
    DseGraphAttributesBuilder(attr.copy(graphChecks = check :: attr.graphChecks))
  def check(check: GenericCheck):DseGraphAttributesBuilder[T] =
    DseGraphAttributesBuilder(attr.copy(genericChecks = check :: attr.genericChecks))
}
