/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.request

import com.datastax.driver.core.{ConsistencyLevel, PagingState}
import com.datastax.driver.core.policies.RetryPolicy
import com.datastax.gatling.plugin.checks.{DseCqlCheck, GenericCheck}
import com.datastax.gatling.plugin.response.CqlResponse
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.check.Check


/**
  * Request Builder for CQL Requests
  *
  * @param attr Addition Attributes
  */
case class DseCqlRequestBuilder(attr: DseCqlAttributes) {
  /**
    * Builds to final action to run
    *
    * @return
    */
  def build(): ActionBuilder = new DseCqlRequestActionBuilder(attr)

  /**
    * Set Consistency Level
    *
    * @param level ConsistencyLevel
    * @return
    */
  def withConsistencyLevel(level: ConsistencyLevel) = DseCqlRequestBuilder(attr.copy(cl = Some(level)))

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
  def withUserOrRole(userOrRole: String) = DseCqlRequestBuilder(attr.copy(userOrRole = Some(userOrRole)))

  /**
    * Override the current system time for write time of query
    *
    * @param epochTsInMs timestamp to use
    * @return
    */
  def withDefaultTimestamp(epochTsInMs: Long) = DseCqlRequestBuilder(attr.copy(defaultTimestamp = Some(epochTsInMs)))


  /**
    * Set query to be idempotent i.e. run only once
    *
    * @return
    */
  def withIdempotency() = DseCqlRequestBuilder(attr.copy(idempotent = Some(true)))


  /**
    * Set Read timeout of the query
    *
    * @param readTimeoutInMs time in milliseconds
    * @return
    */
  def withReadTimeout(readTimeoutInMs: Int) = DseCqlRequestBuilder(attr.copy(readTimeout = Some(readTimeoutInMs)))


  /**
    * Set Serial Consistency
    *
    * @param level ConsistencyLevel
    * @return
    */
  def withSerialConsistencyLevel(level: ConsistencyLevel) = DseCqlRequestBuilder(attr.copy(serialCl = Some(level)))


  /**
    * Define the [[com.datastax.driver.core.policies.RetryPolicy]] to be used for query
    *
    * @param retryPolicy DataStax drivers retry policy
    * @return
    */
  def withRetryPolicy(retryPolicy: RetryPolicy) = DseCqlRequestBuilder(attr.copy(retryPolicy = Some(retryPolicy)))

  /**
    * Set fetchSize of query for paging
    *
    * @param rowCnt number of rows to fetch at one time
    * @return
    */
  def withFetchSize(rowCnt: Int) = DseCqlRequestBuilder(attr.copy(fetchSize = Some(rowCnt)))


  /**
    * Enable CQL Tracing on the query
    *
    * @return
    */
  def withTracingEnabled() = DseCqlRequestBuilder(attr.copy(enableTrace = Some(true)))


  /**
    * Set the paging state
    *
    * @param pagingState CQL Paging state
    * @return
    */
  def withPagingState(pagingState: PagingState) = DseCqlRequestBuilder(attr.copy(pagingState = Some(pagingState)))


  /**
    * For backwards compatibility
    *
    * @param level
    * @return
    */
  def serialConsistencyLevel(level: ConsistencyLevel) = withSerialConsistencyLevel(level)


  /**
    * Backwards compatibility to set consistencyLevel
    *
    * @deprecated
    * @see [[DseCqlRequestBuilder.withConsistencyLevel]]
    * @param level Consistency Level to use
    * @return
    */
  def consistencyLevel(level: ConsistencyLevel) = withConsistencyLevel(level)


  /**
    * For Backwards compatibility
    *
    * @deprecated
    * @see [[DseCqlRequestBuilder.executeAs]]
    * @param userOrRole User or role to use
    * @return
    */
  def executeAs(userOrRole: String) = withUserOrRole(userOrRole: String)

  def check(check: DseCqlCheck) = DseCqlRequestBuilder(attr.copy(cqlChecks = check :: attr.cqlChecks))
  def check(check: GenericCheck) = DseCqlRequestBuilder(attr.copy(genericChecks = check :: attr.genericChecks))
}
