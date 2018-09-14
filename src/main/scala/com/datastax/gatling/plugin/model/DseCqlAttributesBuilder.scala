/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.model

import com.datastax.driver.core.policies.RetryPolicy
import com.datastax.driver.core.{ConsistencyLevel, PagingState}
import com.datastax.gatling.plugin.checks.{DseCqlCheck, GenericCheck}
import com.datastax.gatling.plugin.request.CqlRequestActionBuilder
import io.gatling.core.action.builder.ActionBuilder


/**
  * Request Builder for CQL Requests
  *
  * @param attr Addition Attributes
  */
case class DseCqlAttributesBuilder(attr: DseCqlAttributes) {
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
  def withConsistencyLevel(level: ConsistencyLevel) = DseCqlAttributesBuilder(attr.copy(cl = Some(level)))

  /**
    * Set Consistency Level
    *
    * @param dynamicLevel ConsistencyLevel
    * @return
    */
  def withDynamicConsistencyLevel(dynamicLevel: () => ConsistencyLevel) = DseCqlAttributesBuilder(attr.copy(dynamicCl = Some(dynamicLevel)))

  /**
    * Execute a query as another user or another role, provided the current logged in user has PROXY.EXECUTE permission.
    *
    * This permission MUST be granted to the currently logged in user using the CQL statement: `GRANT PROXY.EXECUTE ON
    * ROLE someRole TO alice`.  The user MUST be logged in with
    * [[com.datastax.driver.dse.auth.DsePlainTextAuthProvider]] or
    * [[com.datastax.driver.dse.auth.DseGSSAPIAuthProvider]]
    *
    * @param userOrRole String
    * @return
    */
  def withUserOrRole(userOrRole: String) = DseCqlAttributesBuilder(attr.copy(userOrRole = Some(userOrRole)))

  /**
    * Override the current system time for write time of query
    *
    * @param epochTsInMs timestamp to use
    * @return
    */
  def withDefaultTimestamp(epochTsInMs: Long) = DseCqlAttributesBuilder(attr.copy(defaultTimestamp = Some(epochTsInMs)))


  /**
    * Set query to be idempotent i.e. run only once
    *
    * @return
    */
  def withIdempotency() = DseCqlAttributesBuilder(attr.copy(idempotent = Some(true)))


  /**
    * Set Read timeout of the query
    *
    * @param readTimeoutInMs time in milliseconds
    * @return
    */
  def withReadTimeout(readTimeoutInMs: Int) = DseCqlAttributesBuilder(attr.copy(readTimeout = Some(readTimeoutInMs)))


  /**
    * Set Serial Consistency
    *
    * @param level ConsistencyLevel
    * @return
    */
  def withSerialConsistencyLevel(level: ConsistencyLevel) = DseCqlAttributesBuilder(attr.copy(serialCl = Some(level)))


  /**
    * Define the [[com.datastax.driver.core.policies.RetryPolicy]] to be used for query
    *
    * @param retryPolicy DataStax drivers retry policy
    * @return
    */
  def withRetryPolicy(retryPolicy: RetryPolicy) = DseCqlAttributesBuilder(attr.copy(retryPolicy = Some(retryPolicy)))

  /**
    * Set fetchSize of query for paging
    *
    * @param rowCnt number of rows to fetch at one time
    * @return
    */
  def withFetchSize(rowCnt: Int) = DseCqlAttributesBuilder(attr.copy(fetchSize = Some(rowCnt)))


  /**
    * Enable CQL Tracing on the query
    *
    * @return
    */
  def withTracingEnabled() = DseCqlAttributesBuilder(attr.copy(enableTrace = Some(true)))


  /**
    * Set the paging state
    *
    * @param pagingState CQL Paging state
    * @return
    */
  def withPagingState(pagingState: PagingState) = DseCqlAttributesBuilder(attr.copy(pagingState = Some(pagingState)))


  /**
    * For backwards compatibility
    *
    * @param level
    * @return
    */
  @deprecated("Replaced by withSerialConsistencyLevel")
  def serialConsistencyLevel(level: ConsistencyLevel) = withSerialConsistencyLevel(level)


  /**
    * Backwards compatibility to set consistencyLevel
    *
    * @see [[DseCqlAttributesBuilder.withConsistencyLevel]]
    * @param level Consistency Level to use
    * @return
    */
  @deprecated("Replaced by withConsistencyLevel")
  def consistencyLevel(level: ConsistencyLevel) = withConsistencyLevel(level)


  /**
    * For Backwards compatibility
    *
    * @see [[DseCqlAttributesBuilder.executeAs]]
    * @param userOrRole User or role to use
    * @return
    */
  @deprecated("Replaced by withUserOrRole")
  def executeAs(userOrRole: String) = withUserOrRole(userOrRole: String)

  def check(check: DseCqlCheck) = DseCqlAttributesBuilder(attr.copy(cqlChecks = check :: attr.cqlChecks))

  def check(check: GenericCheck) = DseCqlAttributesBuilder(attr.copy(genericChecks = check :: attr.genericChecks))
}
