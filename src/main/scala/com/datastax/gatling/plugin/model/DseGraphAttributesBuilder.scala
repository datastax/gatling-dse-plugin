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
import com.datastax.gatling.plugin.request.GraphRequestActionBuilder
import io.gatling.core.action.builder.ActionBuilder

import com.datastax.oss.driver.shaded.guava.common.base.Function

/**
  * Request Builder for Graph Requests
  *
  * @param attr Addition Attributes
  */
case class DseGraphAttributesBuilder[T <: GraphStatement[_]](attr: DseGraphAttributes[T]) {
  /**
    * Builds to final action to run
    *
    * @return
    */
  def build(): GraphRequestActionBuilder[T] = new GraphRequestActionBuilder(attr)

  /**
    * Set Consistency Level
    *
    * @param level ConsistencyLevel
    * @return
    */
  def withConsistencyLevel(level: ConsistencyLevel):DseGraphAttributesBuilder[T] =
    DseGraphAttributesBuilder(attr.copy(cl = Some(level)))

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
  def withUserOrRole(userOrRole: String):DseGraphAttributesBuilder[T] =
    DseGraphAttributesBuilder(attr.copy(userOrRole = Some(userOrRole)))

  /**
    * Override the current system time for write time of query
    *
    * @param epochTsInMs timestamp to use
    * @return
    */
  def withDefaultTimestamp(epochTsInMs: Long):DseGraphAttributesBuilder[T] =
    DseGraphAttributesBuilder(attr.copy(defaultTimestamp = Some(epochTsInMs)))

  /**
    * Set query to be idempotent i.e. run only once
    *
    * @return
    */
  def withIdempotency():DseGraphAttributesBuilder[T] =
    DseGraphAttributesBuilder(attr.copy(idempotent = Some(true)))

  /**
    * Set Read timeout of the query
    *
    * @param readTimeoutInMs time in milliseconds
    * @return
    */
  def withReadTimeout(readTimeoutInMs: Int):DseGraphAttributesBuilder[T] =
    DseGraphAttributesBuilder(attr.copy(readTimeout = Some(readTimeoutInMs)))

  /**
    * Sets the graph language
    *
    * @param language graph language to use
    * @return
    */
  def withLanguage(language: String):DseGraphAttributesBuilder[T] =
    DseGraphAttributesBuilder(attr.copy(graphLanguage = Some(language)))

  /**
    * Sets the graph name to use
    *
    * @param name Graph name
    * @return
    */
  def withName(name: String):DseGraphAttributesBuilder[T] =
    DseGraphAttributesBuilder(attr.copy(graphName = Some(name)))

  /**
    * Set the source of the graph
    *
    * @param source graph source
    * @return
    */
  def withSource(source: String):DseGraphAttributesBuilder[T] =
    DseGraphAttributesBuilder(attr.copy(graphSource = Some(source)))

  /**
    * Set the query to be system level
    *
    * @return
    */
  def withSystemQuery():DseGraphAttributesBuilder[T] =
    DseGraphAttributesBuilder(attr.copy(isSystemQuery = Some(true)))

  /**
    * Set Options on graph
    *
    * @param options options in key/value par to set against the query
    * @return
    */
  def withOptions(options: (String, String)*):DseGraphAttributesBuilder[T] =
    DseGraphAttributesBuilder(attr.copy(graphInternalOptions = Some(options)))

  /**
    * Set Option on graph
    *
    * @param option options in key/value par to set against the query
    * @return
    */
  def withOption(option: (String, String)):DseGraphAttributesBuilder[T] = withOptions(option)

  /**
    * Transform results function
    *
    * @param transform Transform Function
    * @return
    */
  def withTransformResults(transform: Function[Row, GraphNode]):DseGraphAttributesBuilder[T] = {
    DseGraphAttributesBuilder(attr.copy(graphTransformResults = Some(transform)))
  }

  /**
    * Define [[ConsistencyLevel]] to be used for read queries
    *
    * @param readCL Consistency Level to use
    * @return
    */
  def withReadConsistency(readCL: ConsistencyLevel):DseGraphAttributesBuilder[T] =
    DseGraphAttributesBuilder(attr.copy(readCL = Some(readCL)))

  /**
    * Define [[ConsistencyLevel]] to be used for write queries
    *
    * @param writeCL Consistency Level to use
    * @return
    */
  def withWriteConsistency(writeCL: ConsistencyLevel):DseGraphAttributesBuilder[T] =
    DseGraphAttributesBuilder(attr.copy(writeCL = Some(writeCL)))

  /**
    * Backwards compatibility to set consistencyLevel
    *
    * @see [[DseGraphAttributesBuilder.withConsistencyLevel]]
    * @param level Consistency Level to use
    * @return
    */
  @deprecated("use withConsistencyLevel() instead, will be removed in future version")
  def consistencyLevel(level: ConsistencyLevel):DseGraphAttributesBuilder[T] = withConsistencyLevel(level)

  /**
    * For Backwards compatibility
    *
    * @see [[DseGraphAttributesBuilder.executeAs]]
    * @param userOrRole User or role to use
    * @return
    */
  @deprecated("use withUserOrRole() instead, will be removed in future version")
  def executeAs(userOrRole: String):DseGraphAttributesBuilder[T] = withUserOrRole(userOrRole: String)

  def check(check: DseGraphCheck):DseGraphAttributesBuilder[T] =
    DseGraphAttributesBuilder(attr.copy(graphChecks = check :: attr.graphChecks))
  def check(check: GenericCheck):DseGraphAttributesBuilder[T] =
    DseGraphAttributesBuilder(attr.copy(genericChecks = check :: attr.genericChecks))
}
