/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.model

import com.datastax.driver.core.{ConsistencyLevel, Row}
import com.datastax.driver.dse.graph.GraphNode
import com.datastax.gatling.plugin.checks.{DseGraphCheck, GenericCheck}
import com.datastax.gatling.plugin.request.GraphRequestActionBuilder
import io.gatling.core.action.builder.ActionBuilder


/**
  * Request Builder for Graph Requests
  *
  * @param attr Addition Attributes
  */
case class DseGraphAttributesBuilder(attr: DseGraphAttributes) {
  /**
    * Builds to final action to run
    *
    * @return
    */
  def build(): ActionBuilder = new GraphRequestActionBuilder(attr)

  /**
    * Set Consistency Level
    *
    * @param level ConsistencyLevel
    * @return
    */
  def withConsistencyLevel(level: ConsistencyLevel) = DseGraphAttributesBuilder(attr.copy(cl = Some(level)))

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
  def withUserOrRole(userOrRole: String) = DseGraphAttributesBuilder(attr.copy(userOrRole = Some(userOrRole)))

  /**
    * Override the current system time for write time of query
    *
    * @param epochTsInMs timestamp to use
    * @return
    */
  def withDefaultTimestamp(epochTsInMs: Long) = DseGraphAttributesBuilder(attr.copy(defaultTimestamp = Some(epochTsInMs)))


  /**
    * Set query to be idempotent i.e. run only once
    *
    * @return
    */
  def withIdempotency() = DseGraphAttributesBuilder(attr.copy(idempotent = Some(true)))


  /**
    * Set Read timeout of the query
    *
    * @param readTimeoutInMs time in milliseconds
    * @return
    */
  def withReadTimeout(readTimeoutInMs: Int) = DseGraphAttributesBuilder(attr.copy(readTimeout = Some(readTimeoutInMs)))


  /**
    * Sets the graph language
    *
    * @param language graph language to use
    * @return
    */
  def withLanguage(language: String) = DseGraphAttributesBuilder(attr.copy(graphLanguage = Some(language)))

  /**
    * Sets the graph name to use
    *
    * @param name Graph name
    * @return
    */
  def withName(name: String) = DseGraphAttributesBuilder(attr.copy(graphName = Some(name)))


  /**
    * Set the source of the graph
    *
    * @param source graph source
    * @return
    */
  def withSource(source: String) = DseGraphAttributesBuilder(attr.copy(graphSource = Some(source)))

  /**
    * Set the query to be system level
    *
    * @return
    */
  def withSystemQuery() = DseGraphAttributesBuilder(attr.copy(isSystemQuery = Some(true)))

  /**
    * Set Options on graph
    *
    * @param options options in key/value par to set against the query
    * @return
    */
  def withOptions(options: (String, String)*) = DseGraphAttributesBuilder(attr.copy(graphInternalOptions = Some(options)))


  /**
    * Set Option on graph
    *
    * @param option options in key/value par to set against the query
    * @return
    */
  def withOption(option: (String, String)) = withOptions(option)


  /**
    * Transform results function
    *
    * @param transform Transform Function
    * @return
    */
  def withTransformResults(transform: com.google.common.base.Function[Row, GraphNode]) = {
    DseGraphAttributesBuilder(attr.copy(graphTransformResults = Some(transform)))
  }

  /**
    * Define [[ConsistencyLevel]] to be used for read queries
    *
    * @param readCL Consistency Level to use
    * @return
    */
  def withReadConsistency(readCL: ConsistencyLevel) = DseGraphAttributesBuilder(attr.copy(readCL = Some(readCL)))

  /**
    * Define [[ConsistencyLevel]] to be used for write queries
    *
    * @param writeCL Consistency Level to use
    * @return
    */
  def withWriteConsistency(writeCL: ConsistencyLevel) = DseGraphAttributesBuilder(attr.copy(writeCL = Some(writeCL)))


  /**
    * Backwards compatibility to set consistencyLevel
    *
    * @deprecated
    * @see [[DseGraphAttributesBuilder.withConsistencyLevel]]
    * @param level Consistency Level to use
    * @return
    */
  def consistencyLevel(level: ConsistencyLevel) = withConsistencyLevel(level)


  /**
    * For Backwards compatibility
    *
    * @deprecated
    * @see [[DseGraphAttributesBuilder.executeAs]]
    * @param userOrRole User or role to use
    * @return
    */
  def executeAs(userOrRole: String) = withUserOrRole(userOrRole: String)

  def check(check: DseGraphCheck) = DseGraphAttributesBuilder(attr.copy(graphChecks = check :: attr.graphChecks))
  def check(check: GenericCheck) = DseGraphAttributesBuilder(attr.copy(genericChecks = check :: attr.genericChecks))
}
