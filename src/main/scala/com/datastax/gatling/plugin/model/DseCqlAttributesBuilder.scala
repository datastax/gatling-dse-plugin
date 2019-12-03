/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.model

import java.nio.ByteBuffer

import com.datastax.oss.driver.api.core.retry.RetryPolicy
import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.datastax.gatling.plugin.checks.{DseCqlCheck, GenericCheck}
import com.datastax.gatling.plugin.request.CqlRequestActionBuilder
import com.datastax.oss.driver.api.core.cql.Statement


/**
  * Request Builder for CQL Requests
  *
  * @param attr Addition Attributes
  */
case class DseCqlAttributesBuilder[T <: Statement[_]](attr: DseCqlAttributes[T]) {
  /**
    * Builds to final action to run
    *
    * @return
    */
  def build(): CqlRequestActionBuilder = new CqlRequestActionBuilder(attr)

  /**
    * Set Consistency Level
    *
    * @param level ConsistencyLevel
    * @return
    */
  def withConsistencyLevel(level: ConsistencyLevel):DseCqlAttributesBuilder[T] =
    DseCqlAttributesBuilder(attr.copy(cl = Some(level)))

  /**
    * Execute a query as another user or another role, provided the current logged in user has PROXY.EXECUTE permission.
    *
    * This permission MUST be granted to the currently logged in user using the CQL statement: `GRANT PROXY.EXECUTE ON
    * ROLE someRole TO alice`.  The user MUST be logged in with
    * [[com.datastax.dse.driver.api.core.auth.DsePlainTextAuthProviderBase]] or
    * [[com.datastax.dse.driver.api.core.auth.DseGssApiAuthProviderBase]]
    *
    * @param userOrRole String
    * @return
    */
  def withUserOrRole(userOrRole: String):DseCqlAttributesBuilder[T] =
    DseCqlAttributesBuilder(attr.copy(userOrRole = Some(userOrRole)))

  /**
    * Override the current system time for write time of query
    *
    * @param epochTsInMs timestamp to use
    * @return
    */
  def withDefaultTimestamp(epochTsInMs: Long):DseCqlAttributesBuilder[T] =
    DseCqlAttributesBuilder(attr.copy(defaultTimestamp = Some(epochTsInMs)))


  /**
    * Set query to be idempotent i.e. run only once
    *
    * @return
    */
  def withIdempotency():DseCqlAttributesBuilder[T] =
    DseCqlAttributesBuilder(attr.copy(idempotent = Some(true)))


  /**
    * Set Read timeout of the query
    *
    * @param readTimeoutInMs time in milliseconds
    * @return
    */
  def withReadTimeout(readTimeoutInMs: Int):DseCqlAttributesBuilder[T] =
    DseCqlAttributesBuilder(attr.copy(readTimeout = Some(readTimeoutInMs)))


  /**
    * Set Serial Consistency
    *
    * @param level ConsistencyLevel
    * @return
    */
  def withSerialConsistencyLevel(level: ConsistencyLevel):DseCqlAttributesBuilder[T] =
    DseCqlAttributesBuilder(attr.copy(serialCl = Some(level)))


  /**
    * Define the [[com.datastax.oss.driver.api.core.retry.RetryPolicy]] to be used for query
    *
    * @param retryPolicy DataStax drivers retry policy
    * @return
    */
  def withRetryPolicy(retryPolicy: RetryPolicy):DseCqlAttributesBuilder[T] =
    DseCqlAttributesBuilder(attr.copy(retryPolicy = Some(retryPolicy)))

  /**
    * Set fetchSize of query for paging
    *
    * @param rowCnt number of rows to fetch at one time
    * @return
    */
  def withFetchSize(rowCnt: Int):DseCqlAttributesBuilder[T] =
    DseCqlAttributesBuilder(attr.copy(fetchSize = Some(rowCnt)))


  /**
    * Enable CQL Tracing on the query
    *
    * @return
    */
  def withTracingEnabled():DseCqlAttributesBuilder[T] =
    DseCqlAttributesBuilder(attr.copy(enableTrace = Some(true)))


  /**
    * Set the paging state
    *
    * @param pagingState CQL Paging state
    * @return
    */
  def withPagingState(pagingState: ByteBuffer):DseCqlAttributesBuilder[T] =
    DseCqlAttributesBuilder(attr.copy(pagingState = Some(pagingState)))


  /**
    * For backwards compatibility
    *
    * @param level
    * @return
    */
  @deprecated("Replaced by withSerialConsistencyLevel")
  def serialConsistencyLevel(level: ConsistencyLevel):DseCqlAttributesBuilder[T] =
    withSerialConsistencyLevel(level)


  /**
    * Backwards compatibility to set consistencyLevel
    *
    * @see [[DseCqlAttributesBuilder.withConsistencyLevel]]
    * @param level Consistency Level to use
    * @return
    */
  @deprecated("Replaced by withConsistencyLevel")
  def consistencyLevel(level: ConsistencyLevel):DseCqlAttributesBuilder[T] =
    withConsistencyLevel(level)


  /**
    * For Backwards compatibility
    *
    * @see [[DseCqlAttributesBuilder.executeAs]]
    * @param userOrRole User or role to use
    * @return
    */
  @deprecated("Replaced by withUserOrRole")
  def executeAs(userOrRole: String):DseCqlAttributesBuilder[T] =
    withUserOrRole(userOrRole: String)

  def check(check: DseCqlCheck):DseCqlAttributesBuilder[T] =
    DseCqlAttributesBuilder(attr.copy(cqlChecks = check :: attr.cqlChecks))

  def check(check: GenericCheck):DseCqlAttributesBuilder[T] =
    DseCqlAttributesBuilder(attr.copy(genericChecks = check :: attr.genericChecks))
}
