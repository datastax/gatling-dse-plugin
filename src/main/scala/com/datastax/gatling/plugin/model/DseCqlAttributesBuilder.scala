/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.model

import java.nio.ByteBuffer
import java.time.Duration

import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.datastax.oss.driver.api.core.cql.{Statement, StatementBuilder}
import com.datastax.gatling.plugin.checks.DseCqlCheck
import com.datastax.gatling.plugin.request.CqlRequestActionBuilder
import com.datastax.oss.driver.api.core.metadata.Node
import com.datastax.oss.driver.api.core.metadata.token.Token


/**
  * Request Builder for CQL Requests
  *
  * @param attr Addition Attributes
  */
case class DseCqlAttributesBuilder[T <: Statement[T], B <: StatementBuilder[B,T]](attr: DseCqlAttributes[T, B]) {
  /**
    * Builds to final action to run
    *
    * @return
    */
  def build(): CqlRequestActionBuilder[T, B] = new CqlRequestActionBuilder(attr)

  /**
    * Set Consistency Level
    *
    * @param level ConsistencyLevel
    * @return
    */
  def withConsistencyLevel(level: ConsistencyLevel):DseCqlAttributesBuilder[T, B] =
    DseCqlAttributesBuilder(attr.copy(cl = Some(level)))

  /**
    * Set query to be idempotent i.e. run only once
    *
    * @return
    */
  def withIdempotency():DseCqlAttributesBuilder[T, B] =
    DseCqlAttributesBuilder(attr.copy(idempotent = Some(true)))

  /**
    * Set query to be idempotent i.e. run only once
    *
    * @return
    */
  def withIdempotency(idempotency:Boolean):DseCqlAttributesBuilder[T, B] =
    DseCqlAttributesBuilder(attr.copy(idempotent = Some(idempotency)))

  /**
    * Set the node that should handle this query
    * @param node Node
    * @return
    */
  def withNode(node: Node):DseCqlAttributesBuilder[T, B] =
    DseCqlAttributesBuilder(attr.copy(node = Some(node)))

  /**
    * Enable CQL Tracing on the query
    *
    * @return
    */
  def withTracingEnabled():DseCqlAttributesBuilder[T, B] =
    DseCqlAttributesBuilder(attr.copy(enableTrace = Some(true)))

  /**
    * Set the page size
    *
    * @param pageSize CQL page size
    * @return
    */
  def withPageSize(pageSize: Int):DseCqlAttributesBuilder[T, B] =
    DseCqlAttributesBuilder(attr.copy(pageSize = Some(pageSize)))

  /**
    * Set the paging state
    *
    * @param pagingState CQL Paging state
    * @return
    */
  def withPagingState(pagingState: ByteBuffer):DseCqlAttributesBuilder[T, B] =
    DseCqlAttributesBuilder(attr.copy(pagingState = Some(pagingState)))

  /**
    * Set the query timestamp
    *
    * @param queryTimestamp CQL query timestamp
    * @return
    */
  def withQueryTimestamp(queryTimestamp: Long):DseCqlAttributesBuilder[T, B] =
    DseCqlAttributesBuilder(attr.copy(queryTimestamp = Some(queryTimestamp)))

  /**
    * Set the routing key
    *
    * @param routingKey the routing key to use
    * @return
    */
  def withRoutingKey(routingKey: ByteBuffer):DseCqlAttributesBuilder[T, B] =
    DseCqlAttributesBuilder(attr.copy(routingKey = Some(routingKey)))

  /**
    * Set the routing keyspace
    *
    * @param routingKeyspace the routing keyspace to set
    * @return
    */
  def withRoutingKeyspace(routingKeyspace: String):DseCqlAttributesBuilder[T, B] =
    DseCqlAttributesBuilder(attr.copy(routingKeyspace = Some(routingKeyspace)))

  /**
    * Set the routing token
    *
    * @param routingToken the routing token to set
    * @return
    */
  def withRoutingToken(routingToken: Token):DseCqlAttributesBuilder[T, B] =
    DseCqlAttributesBuilder(attr.copy(routingToken = Some(routingToken)))

  /**
    * Set Serial Consistency
    *
    * @param level ConsistencyLevel
    * @return
    */
  def withSerialConsistencyLevel(level: ConsistencyLevel):DseCqlAttributesBuilder[T, B] =
    DseCqlAttributesBuilder(attr.copy(serialCl = Some(level)))

  /**
    * Set timeout
    *
    * @param timeout the timeout to set
    * @return
    */
  def withTimeout(timeout: Duration):DseCqlAttributesBuilder[T, B] =
    DseCqlAttributesBuilder(attr.copy(timeout = Some(timeout)))

  /**
    * For backwards compatibility
    *
    * @param level
    * @return
    */
  @deprecated("Replaced by withSerialConsistencyLevel")
  def serialConsistencyLevel(level: ConsistencyLevel):DseCqlAttributesBuilder[T, B] =
    withSerialConsistencyLevel(level)

  /**
    * Backwards compatibility to set consistencyLevel
    *
    * @see [[DseCqlAttributesBuilder.withConsistencyLevel]]
    * @param level Consistency Level to use
    * @return
    */
  @deprecated("Replaced by withConsistencyLevel")
  def consistencyLevel(level: ConsistencyLevel):DseCqlAttributesBuilder[T, B] =
    withConsistencyLevel(level)

  def check(check: DseCqlCheck):DseCqlAttributesBuilder[T, B] =
    DseCqlAttributesBuilder(attr.copy(cqlChecks = (attr.cqlChecks :+ check)))
}
