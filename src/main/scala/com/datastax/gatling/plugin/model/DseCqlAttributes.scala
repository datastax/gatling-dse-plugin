/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.model

import java.nio.ByteBuffer
import java.time.Duration

import com.datastax.gatling.plugin.checks.DseCqlCheck
import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.datastax.oss.driver.api.core.cql.{Statement, StatementBuilder}
import com.datastax.oss.driver.api.core.metadata.Node
import com.datastax.oss.driver.api.core.metadata.token.Token
import com.datastax.oss.driver.api.core.time.TimestampGenerator

/**
  * CQL Query Attributes to be applied to the current query
  *
  * See [[Statement]] for documentation on each option.
  *
  * @param tag              Name of Query to include in reports
  * @param statement        CQL Statement to be sent to Cluster
  * @param cl               Consistency Level to be used
  * @param cqlChecks        Data-level checks to be run after response is returned
  * @param idempotent       Set request to be idempotent i.e. whether it can be applied multiple times
  * @param node             Set the node that should handle this query
  * @param userOrRole       Set the user/role for this query if proxy authentication is used
  * @param customPayload    Custom payload for this request
  * @param enableTrace      Whether tracing should be enabled
  * @param pageSize         Set pageSize (formerly known as fetchSize)
  * @param pagingState      Set paging State wanted
  * @param queryTimestamp   Set a timestamp to use for this query.  If equal to Some(Long.MIN_VALUE) a timestamp
  *                         will be generated by the configured [[TimestampGenerator]]
  * @param routingKey       Sets the key for token-aware routing
  * @param routingKeyspace  Sets the keyspace for token-aware routing
  * @param routingToken     Sets the token to use for token-aware routing
  * @param serialCl         Serial Consistency Level to be used
  * @param timeout          How long to wait for this request to complete
  * @param cqlStatements    String version of the CQL statement that is sent
  *
  */
case class DseCqlAttributes[T <: Statement[T], B <: StatementBuilder[B,T]]
  (tag: String,
   statement: DseCqlStatement[T, B],
   cqlChecks: List[DseCqlCheck] = List.empty,
   cqlStatements: Seq[String] = Seq.empty,
    /* General attributes */
   cl: Option[ConsistencyLevel] = None,
   idempotent: Option[Boolean] = None,
   node: Option[Node] = None,
   userOrRole: Option[String] = None,
   /* CQL-specific attributes */
   customPayload: Option[Map[String, ByteBuffer]] = None,
   enableTrace: Option[Boolean] = None,
   pageSize: Option[Int] = None,
   pagingState: Option[ByteBuffer] = None,
   queryTimestamp: Option[Long] = None,
   routingKey: Option[ByteBuffer] = None,
   routingKeyspace: Option[String] = None,
   routingToken: Option[Token] = None,
   serialCl: Option[ConsistencyLevel] = None,
   timeout: Option[Duration] = None)
