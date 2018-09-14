/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.model

import java.nio.ByteBuffer

import com.datastax.driver.core.policies.RetryPolicy
import com.datastax.driver.core.{ConsistencyLevel, PagingState, Statement}
import com.datastax.gatling.plugin.response.{CqlResponse, DseResponse}
import io.gatling.core.check.Check

/**
  * CQL Query Attributes to be applied to the current query
  *
  * See [[Statement]] for documentation on each option.
  *
  * @param tag              Name of Query to include in reports
  * @param statement        CQL Statement to be sent to Cluster
  * @param cl               Consistency Level to be used
  * @param dynamicCl         Consistency Level to be used, will be called on every request
  * @param cqlChecks        Data-level checks to be run after response is returned
  * @param genericChecks    Low-level checks to be run after response is returned
  * @param userOrRole       User or role to be used when proxy auth is enabled
  * @param readTimeout      Read timeout to be used
  * @param idempotent       Set request to be idempotent i.e. whether it can be applied multiple times
  * @param defaultTimestamp Set default timestamp on request, overriding current system time
  * @param enableTrace      Whether tracing should be enabled
  * @param outGoingPayload  Set ByteBuffer custom outgoing Payload
  * @param serialCl         Serial Consistency Level to be used
  * @param fetchSize        Set fetchSize of request i.e. paging size
  * @param retryPolicy      Retry Policy to be used
  * @param pagingState      Set paging State wanted
  * @param cqlStatements    String version of the CQL statement that is sent
  *
  */
case class DseCqlAttributes(tag: String,
                            statement: DseStatement[Statement],
                            cl: Option[ConsistencyLevel] = None,
                            dynamicCl: Option[() => ConsistencyLevel] = None,
                            cqlChecks: List[Check[CqlResponse]] = List.empty,
                            genericChecks: List[Check[DseResponse]] = List.empty,
                            userOrRole: Option[String] = None,
                            readTimeout: Option[Int] = None,
                            idempotent: Option[Boolean] = None,
                            defaultTimestamp: Option[Long] = None,
                            enableTrace: Option[Boolean] = None,
                            outGoingPayload: Option[Map[String, ByteBuffer]] = None,
                            serialCl: Option[ConsistencyLevel] = None,
                            fetchSize: Option[Int] = None,
                            retryPolicy: Option[RetryPolicy] = None,
                            pagingState: Option[PagingState] = None,
                            cqlStatements: Seq[String] = Seq.empty)
