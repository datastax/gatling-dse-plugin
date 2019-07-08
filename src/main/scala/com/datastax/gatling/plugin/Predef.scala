/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin

import com.datastax.dse.driver.api.core.graph.{GraphExecutionInfo, ScriptGraphStatement}
import com.datastax.gatling.plugin.checks.{CqlChecks, CqlGenericChecks, DseCheckSupport, GraphGenericChecks}
import com.datastax.gatling.plugin.model.{DseCqlAttributesBuilder, DseCqlStatementBuilder, DseGraphAttributesBuilder, DseGraphStatementBuilder}
import com.datastax.gatling.plugin.request._
import com.datastax.oss.driver.api.core.cql.{ExecutionInfo, SimpleStatement}
import io.gatling.core.action.builder.ActionBuilder

import scala.language.implicitConversions


trait DsePredefBase extends DseCheckSupport {

  val dseProtocolBuilder: DseProtocolBuilder.type = DseProtocolBuilder

  /**
    * Present for backwards compatibility
    */
  @deprecated("use dseProtocolBuilder instead, will be removed in future versions")
  val graph: DseProtocolBuilder.type = dseProtocolBuilder

  /**
    * Present for backwards compatibility
    */
  @deprecated("use dseProtocolBuilder instead, will be removed in future versions")
  val cql: DseProtocolBuilder.type = dseProtocolBuilder

  def cql(tag: String): DseCqlStatementBuilder[SimpleStatement] = DseCqlStatementBuilder(tag)

  def graph(tag: String): DseGraphStatementBuilder = DseGraphStatementBuilder(tag)

  implicit def protocolBuilder2DseProtocol(builder: DseProtocolBuilder): DseProtocol = builder.build

  implicit def cqlRequestAttributes2ActionBuilder(builder: DseCqlAttributesBuilder[SimpleStatement]): ActionBuilder = builder.build()

  implicit def graphRequestAttributes2ActionBuilder(builder: DseGraphAttributesBuilder[ScriptGraphStatement]): ActionBuilder = builder.build()
}

/**
  * DsePredef which should be used for both
  */
object DsePredef extends DsePredefBase {}

object CqlPredef extends DsePredefBase {
  // start global checks
  lazy val exhausted = CqlGenericChecks.exhausted
  lazy val applied = CqlGenericChecks.applied
  lazy val rowCount = CqlGenericChecks.rowCount

  // execution info and subsets
  lazy val executionInfo = CqlGenericChecks.executionInfo
  lazy val achievedCL = CqlGenericChecks.achievedConsistencyLevel
  lazy val pagingState = CqlGenericChecks.pagingState
  lazy val queriedHost = CqlGenericChecks.queriedHost
  lazy val schemaAgreement = CqlGenericChecks.schemaInAgreement
  lazy val successfulExecutionIndex = CqlGenericChecks.successfulExecutionIndex
  lazy val triedHosts = CqlGenericChecks.triedHosts
  lazy val warnings = CqlGenericChecks.warnings
}

object GraphPredef extends DsePredefBase {
  // start global checks
  lazy val exhausted = GraphGenericChecks.exhausted
  lazy val applied = GraphGenericChecks.applied
  lazy val rowCount = GraphGenericChecks.rowCount

  // execution info and subsets
  lazy val executionInfo = GraphGenericChecks.executionInfo
  lazy val achievedCL = GraphGenericChecks.achievedConsistencyLevel
  lazy val pagingState = GraphGenericChecks.pagingState
  lazy val queriedHost = GraphGenericChecks.queriedHost
  lazy val schemaAgreement = GraphGenericChecks.schemaInAgreement
  lazy val successfulExecutionIndex = GraphGenericChecks.successfulExecutionIndex
  lazy val triedHosts = GraphGenericChecks.triedHosts
  lazy val warnings = GraphGenericChecks.warnings

}


