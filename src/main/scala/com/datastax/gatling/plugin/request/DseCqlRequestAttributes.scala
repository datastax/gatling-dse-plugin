package com.datastax.gatling.plugin.request

import com.datastax.driver.core.{ConsistencyLevel, PagingState}
import com.datastax.driver.core.policies.RetryPolicy
import com.datastax.gatling.plugin.checks.DseCheck
import io.gatling.core.action.builder.ActionBuilder


/**
  * Request Builder for CQL Requests
  *
  * @param attr Addition Attributes
  */
case class DseCqlRequestAttributes(attr: DseAttributes) {


  /**
    * Add a single Check to run on response
    *
    * @param check that will be performed on the response
    * @return
    */
  def withCheck(check: DseCheck) = withChecks(check: DseCheck)


  /**
    * Add multiple checks to run on response
    *
    * @param checks checks that will be performed on the response
    * @return
    */
  def withChecks(checks: DseCheck*) = DseCqlRequestAttributes(attr.copy(checks = attr.checks ::: checks.toList))


  /**
    * Builds to final action to run
    *
    * @return
    */
  def build(): ActionBuilder = new DseRequestActionBuilder(attr)

  /**
    * Set Consistency Level
    *
    * @param level ConsistencyLevel
    * @return
    */
  def withConsistencyLevel(level: ConsistencyLevel) = DseCqlRequestAttributes(attr.copy(cl = Some(level)))

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
  def withUserOrRole(userOrRole: String) = DseCqlRequestAttributes(attr.copy(userOrRole = Some(userOrRole)))

  /**
    * Override the current system time for write time of query
    *
    * @param epochTsInMs timestamp to use
    * @return
    */
  def withDefaultTimestamp(epochTsInMs: Long) = DseCqlRequestAttributes(attr.copy(defaultTimestamp = Some(epochTsInMs)))


  /**
    * Set query to be idempotent i.e. run only once
    *
    * @return
    */
  def withIdempotency() = DseCqlRequestAttributes(attr.copy(idempotent = Some(true)))


  /**
    * Set Read timeout of the query
    *
    * @param readTimeoutInMs time in milliseconds
    * @return
    */
  def withReadTimeout(readTimeoutInMs: Int) = DseCqlRequestAttributes(attr.copy(readTimeout = Some(readTimeoutInMs)))


  /**
    * Set Serial Consistency
    *
    * @param level ConsistencyLevel
    * @return
    */
  def withSerialConsistencyLevel(level: ConsistencyLevel) = DseCqlRequestAttributes(attr.copy(cqlSerialCl = Some(level)))


  /**
    * Define the [[com.datastax.driver.core.policies.RetryPolicy]] to be used for query
    *
    * @param retryPolicy DataStax drivers retry policy
    * @return
    */
  def withRetryPolicy(retryPolicy: RetryPolicy) = DseCqlRequestAttributes(attr.copy(cqlRetryPolicy = Some(retryPolicy)))

  /**
    * Set fetchSize of query for paging
    *
    * @param rowCnt number of rows to fetch at one time
    * @return
    */
  def withFetchSize(rowCnt: Int) = DseCqlRequestAttributes(attr.copy(cqlFetchSize = Some(rowCnt)))


  /**
    * Enable CQL Tracing on the query
    *
    * @return
    */
  def withTracingEnabled() = DseCqlRequestAttributes(attr.copy(cqlEnableTrace = Some(true)))


  /**
    * Set the paging state
    *
    * @param pagingState CQL Paging state
    * @return
    */
  def withPagingState(pagingState: PagingState) = DseCqlRequestAttributes(attr.copy(cqlPagingState = Some(pagingState)))


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
    * @see [[DseCqlRequestAttributes.withConsistencyLevel]]
    *
    * @param level Consistency Level to use
    * @return
    */
  def consistencyLevel(level: ConsistencyLevel) = withConsistencyLevel(level)


  /**
    * For Backwards compatibility
    *
    * @deprecated
    * @see [[DseCqlRequestAttributes.executeAs]]
    *
    * @param userOrRole User or role to use
    * @return
    */
  def executeAs(userOrRole: String) = withUserOrRole(userOrRole: String)

  /**
    *
    * @deprecated
    * @see [[DseCqlRequestAttributes.withCheck()]] [[DseCqlRequestAttributes.withChecks()]]
    *
    * @param checks checks that will be performed on the response
    */
  def check(checks: DseCheck) = withChecks(checks: DseCheck)

}
