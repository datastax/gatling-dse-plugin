/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.model

import java.time.Duration

import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.datastax.dse.driver.api.core.graph.{GraphStatement, GraphStatementBuilderBase}
import com.datastax.gatling.plugin.checks.DseGraphCheck
import com.datastax.gatling.plugin.request.GraphRequestActionBuilder
import com.datastax.oss.driver.api.core.metadata.Node

/**
  * Request Builder for Graph Requests
  *
  * @param attr Addition Attributes
  */
case class DseGraphAttributesBuilder[T <: GraphStatement[T], B <: GraphStatementBuilderBase[B,T]](attr: DseGraphAttributes[T, B]) {
  /**
    * Builds to final action to run
    *
    * @return
    */
  def build(): GraphRequestActionBuilder[T, B] = new GraphRequestActionBuilder(attr)

  /**
    * Set Consistency Level
    *
    * @param level ConsistencyLevel
    * @return
    */
  def withConsistencyLevel(level: ConsistencyLevel):DseGraphAttributesBuilder[T, B] =
    DseGraphAttributesBuilder(attr.copy(cl = Some(level)))

  /**
    * Set query to be idempotent i.e. run only once
    *
    * @return
    */
  def withIdempotency():DseGraphAttributesBuilder[T, B] =
    DseGraphAttributesBuilder(attr.copy(idempotent = Some(true)))

  /**
    * Set the node that should handle this query
    * @param node Node
    * @return
    */
  def withNode(node: Node):DseGraphAttributesBuilder[T, B] =
    DseGraphAttributesBuilder(attr.copy(node = Some(node)))

  /**
    * Set the user or role to use for proxy auth
    * @param userOrRole String
    * @return
    */
  def executeAs(userOrRole: String):DseGraphAttributesBuilder[T, B] =
    DseGraphAttributesBuilder(attr.copy(userOrRole = Some(userOrRole)))


  /**
    * Sets the graph name to use
    *
    * @param name Graph name
    * @return
    */
  def withName(name: String):DseGraphAttributesBuilder[T, B] =
    DseGraphAttributesBuilder(attr.copy(graphName = Some(name)))

  /**
    * Define [[ConsistencyLevel]] to be used for read queries
    *
    * @param readCL Consistency Level to use
    * @return
    */
  def withReadConsistency(readCL: ConsistencyLevel):DseGraphAttributesBuilder[T, B] =
    DseGraphAttributesBuilder(attr.copy(readCL = Some(readCL)))

  /**
    * Set the sub-protocol
    *
    * @param subProtocol the sub-protocol to use
    * @return
    */
  def withSubProtocol(subProtocol: String):DseGraphAttributesBuilder[T, B] =
    DseGraphAttributesBuilder(attr.copy(subProtocol = Some(subProtocol)))

  /**
    * Set the timeout
    *
    * @param timeout the timeout to use
    * @return
    */
  def withTimeout(timeout: Duration):DseGraphAttributesBuilder[T, B] =
    DseGraphAttributesBuilder(attr.copy(timeout = Some(timeout)))

  /**
    * Set the timestamp
    *
    * @param timestamp the timestamp to use
    * @return
    */
  def withTimestamp(timestamp: Long):DseGraphAttributesBuilder[T, B] =
    DseGraphAttributesBuilder(attr.copy(timestamp = Some(timestamp)))

  /**
    * Set the sub-protocol
    *
    * @param traversalSource the traversal source to use
    * @return
    */
  def withTraversalSource(traversalSource: String):DseGraphAttributesBuilder[T, B] =
    DseGraphAttributesBuilder(attr.copy(traversalSource = Some(traversalSource)))

  /**
    * Define [[ConsistencyLevel]] to be used for write queries
    *
    * @param writeCL Consistency Level to use
    * @return
    */
  def withWriteConsistency(writeCL: ConsistencyLevel):DseGraphAttributesBuilder[T, B] =
    DseGraphAttributesBuilder(attr.copy(writeCL = Some(writeCL)))

  def check(check: DseGraphCheck):DseGraphAttributesBuilder[T, B] =
    DseGraphAttributesBuilder(attr.copy(graphChecks = check :: attr.graphChecks))
}
