/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.model

import com.datastax.oss.driver.api.core.ConsistencyLevel
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.dse.driver.api.core.graph.{GraphNode, GraphStatement}
import com.datastax.gatling.plugin.checks.{DseGraphCheck, GenericCheck}

/**
  * Graph Query Attributes to be applied to the current query
  *
  * See [[GraphStatement]] for documentation on each option.
  *
  * @param tag                    Name of Query to include in reports
  * @param statement              Graph Statement to be sent to Cluster
  * @param cl                     Consistency Level to be used
  * @param graphChecks            Data-level checks to be run after response is returned
  * @param genericChecks          Low-level checks to be run after response is returned
  * @param userOrRole             User or role to be used when proxy auth is enabled
  * @param readTimeout            Read timeout to be used
  * @param idempotent             Set request to be idempotent i.e. whether it can be applied multiple times
  * @param defaultTimestamp       Set default timestamp on request, overriding current system time
  * @param  readCL                Consistency level to use for the read part of the query
  * @param  writeCL               Consistency level to use for the write part of the query
  * @param  graphName             Name of the graph to use if different from the one used when connecting
  * @param  graphLanguage         Language used in the query
  * @param  graphSource           Graph source to use if different from the one used when connecting
  * @param  isSystemQuery         Whether the query is a system one and should be used without any graph name
  * @param  graphInternalOptions  Query-specific options not available in the driver public API
  * @param  graphTransformResults Function to use in order to transform a row into a Graph node
  */
case class DseGraphAttributes[T <: GraphStatement[_]](tag: String,
                              statement: DseStatement[T],
                              cl: Option[ConsistencyLevel] = None,
                              graphChecks: List[DseGraphCheck] = List.empty,
                              genericChecks: List[GenericCheck] = List.empty,
                              userOrRole: Option[String] = None,
                              readTimeout: Option[Int] = None,
                              idempotent: Option[Boolean] = None,
                              defaultTimestamp: Option[Long] = None,
                              readCL: Option[ConsistencyLevel] = None,
                              writeCL: Option[ConsistencyLevel] = None,
                              graphName: Option[String] = None,
                              graphLanguage: Option[String] = None,
                              graphSource: Option[String] = None,
                              isSystemQuery: Option[Boolean] = None,
                              graphInternalOptions: Option[Seq[(String, String)]] = None,
                              graphTransformResults: Option[com.datastax.oss.driver.shaded.guava.common.base.Function[Row, GraphNode]] = None)
