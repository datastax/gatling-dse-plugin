/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.request

import java.nio.ByteBuffer

import com.datastax.driver.core.policies.RetryPolicy
import com.datastax.driver.core.{ConsistencyLevel, PagingState, Row}
import com.datastax.driver.dse.graph.GraphNode
import com.datastax.gatling.plugin.checks.DseCheck

/**
  * CQL Query Attributes to be applied to the current query
  *
  * See [[com.datastax.driver.core.Statement]] or [[com.datastax.driver.dse.graph.GraphStatement]]
  * for documentation on each option
  *
  * @param tag                Name of Query to include in reports
  * @param statement          CQL or Graph Statement to be sent to Cluster
  * @param cl                 Consistency Level to be used
  * @param checks             Gatling checks to be run after response is returned
  * @param userOrRole         User or role to be used when proxy auth is enabled
  * @param readTimeout        Read timeout to be used
  * @param idempotent         Set request to be idempotent i.e. whether it can be applied multiple times
  * @param defaultTimestamp   Set default timestamp on request, overriding current system time
  *
  * @param cqlSerialCl        Serial Consistency Level to be used
  * @param cqlFetchSize       Set fetchSize of request i.e. paging size
  * @param cqlRetryPolicy     Retry Policy to be used
  * @param cqlOutGoingPayload Set ByteBuffer custom outgoing Payload
  * @param cqlPagingState     Set paging State wanted
  *
  */
case class DseAttributes(tag: String,
                         statement: Any,
                         cl: Option[ConsistencyLevel] = None,
                         checks: List[DseCheck] = List.empty[DseCheck],
                         userOrRole: Option[String] = None,
                         readTimeout: Option[Int] = None,
                         idempotent: Option[Boolean] = None,
                         defaultTimestamp: Option[Long] = None,

                         // cql specific options
                         cqlEnableTrace: Option[Boolean] = None,
                         cqlOutGoingPayload: Option[Map[String, ByteBuffer]] = None,
                         cqlSerialCl: Option[ConsistencyLevel] = None,
                         cqlFetchSize: Option[Int] = None,
                         cqlRetryPolicy: Option[RetryPolicy] = None,
                         cqlPagingState: Option[PagingState] = None,
                         cqlStatements: Seq[String] = Seq.empty,

                         // graph specific options
                         graphReadCL: Option[ConsistencyLevel] = None,
                         graphWriteCL: Option[ConsistencyLevel] = None,
                         graphName: Option[String] = None,
                         graphLanguage: Option[String] = None,
                         graphSource: Option[String] = None,
                         graphSystemQuery: Option[Boolean] = None,
                         graphInternalOptions: Option[Seq[(String, String)]] = None,
                         graphTransformResults: Option[com.google.common.base.Function[Row, GraphNode]] = None
                        )