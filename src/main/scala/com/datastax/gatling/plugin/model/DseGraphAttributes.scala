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
import com.datastax.oss.driver.api.core.metadata.Node

/**
  * Graph Query Attributes to be applied to the current query
  *
  * See [[GraphStatement]] for documentation on each option.
  *
  * @param tag                    Name of Query to include in reports
  * @param statement              Graph Statement to be sent to Cluster
  * @param cl                     Consistency Level to be used
  * @param graphChecks            Data-level checks to be run after response is returned
  * @param idempotent             Set request to be idempotent i.e. whether it can be applied multiple times
  * @param node                   Set the node that should handle this query
  * @param userOrRole             Set the user/role for this query if proxy authentication is used
  * @param graphName              Name of the graph to use if different from the one used when connecting
  * @param readCL                 Consistency level to use for the read part of the query
  * @param subProtocol            Name of the graph protocol to use for encoding/decoding
  * @param timeout                Timeout to use for this request
  * @param timestamp              Timestamp to use for this request
  * @param traversalSource        The traversal source for this request
  * @param writeCL                Consistency level to use for the write part of the query
  */
case class DseGraphAttributes[T <: GraphStatement[T], B <: GraphStatementBuilderBase[B,T]]
  (tag: String,
   statement: DseGraphStatement[T, B],
   graphChecks: List[DseGraphCheck] = List.empty,
   /* General attributes */
   cl: Option[ConsistencyLevel] = None,
   idempotent: Option[Boolean] = None,
   node: Option[Node] = None,
   userOrRole: Option[String] = None,
   /* Graph-specific attributes */
   graphName: Option[String] = None,
   readCL: Option[ConsistencyLevel] = None,
   subProtocol: Option[String] = None,
   timeout: Option[Duration] = None,
   timestamp: Option[Long] = None,
   traversalSource: Option[String] = None,
   writeCL: Option[ConsistencyLevel] = None)
