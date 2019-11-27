/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.model

import com.datastax.dse.driver.api.core.graph.{FluentGraphStatement, ScriptGraphStatement, ScriptGraphStatementBuilder}
import io.gatling.core.session.{Expression, Session}

/**
  * DSE Graph Request Builder
  *
  * @param tag Graph Query Tag to be included in Reports
  */
case class DseGraphStatementBuilder(tag: String) {

  /**
    * Execute a single string graph query
    *
    * @param strStatement Graph Query String
    * @return
    */
  def executeGraph(strStatement: Expression[String]): DseGraphAttributesBuilder[ScriptGraphStatement] = {
    DseGraphAttributesBuilder(
      DseGraphAttributes(
        tag,
        GraphStringStatement(strStatement)))
  }

  /**
    * Execute a Simple Graph Statement, which can include named params
    *
    * @see DseGraphRequestParamsBuilder#withSetParams
    * @param gStatement Simple Graph Statement
    * @return
    */
  @deprecated("Replaced by executeGraph(ScriptGraphStatement)")
  def executeGraphStatement(gStatement: ScriptGraphStatement): DseGraphParametrizedStatementBuilder =
    executeGraph(gStatement)

  /**
    * Execute a Simple Graph Statement, which can include named params
    *
    * @see DseGraphRequestParamsBuilder#withSetParams
    * @param gStatement Simple Graph Statement
    * @return
    */
  def executeGraph(gStatement: ScriptGraphStatement):DseGraphParametrizedStatementBuilder = {
    DseGraphParametrizedStatementBuilder(tag, new ScriptGraphStatementBuilder(gStatement))
  }

  /**
    * Execute a Fluent Graph API
    *
    * @param gStatement Graph Statement from a Fluent API builder
    * @return
    */
  def executeGraphFluent(gStatement: FluentGraphStatement): DseGraphAttributesBuilder[FluentGraphStatement] = {
    DseGraphAttributesBuilder(
      DseGraphAttributes(
        tag,
        GraphFluentStatement(gStatement)))
  }

  /**
    * Execute a Graph request computed from the Gatling User [[io.gatling.core.session.Session]]
    *
    * This method requires a lambda that produces a Graph statement out of the Session object
    * The request may be created in a Feeder object
    * It may also be created directly in the lambda, from feeder parameters
    *
    * Although any type of query may be used with this method, it should only be used with fluent graph requests
    * Using that method with any other type of request is always wrong
    *
    * @param gLambda The lambda
    * @return
    */
  def executeGraphFluent(gLambda: Session => FluentGraphStatement): DseGraphAttributesBuilder[FluentGraphStatement] = {
    DseGraphAttributesBuilder(
      DseGraphAttributes(
        tag,
        GraphFluentStatementFromScalaLambda(gLambda)))
  }

  /**
    * Execute a traversal previously created by a feeder
    *
    * @param feederKey name of the traversal in the gatling session
    * @return
    */
  @deprecated("Replaced by executeGraphFluent{session => session(feederKey)}")
  def executeGraphFeederTraversal(feederKey: String): DseGraphAttributesBuilder[FluentGraphStatement] = {
    DseGraphAttributesBuilder(
      DseGraphAttributes(
        tag,
        GraphFluentSessionKey(feederKey)))
  }
}

/**
  * Builder for Graph queries that do not have bound parameters yet.
  *
  * @param tag        Query tag
  * @param builder Simple Graph Staetment
  */
case class DseGraphParametrizedStatementBuilder(tag: String, builder: ScriptGraphStatementBuilder) {

  /**
    * Included for compatibility
    *
    * @param paramNames Array of Params names
    * @return
    */
  @deprecated("Replaced by withParams")
  def withSetParams(paramNames: Array[String]): DseGraphAttributesBuilder[ScriptGraphStatement] =
    withParams(paramNames.toList)

  /**
    * Params to set from strings
    *
    * @param paramNames List of strings to use
    * @return
    */
  def withParams(paramNames: String*): DseGraphAttributesBuilder[ScriptGraphStatement] =
    withParams(paramNames.toList)

  /**
    * Params to set from strings
    *
    * @param paramNames List of strings to use
    * @return
    */
  def withParams(paramNames: List[String]): DseGraphAttributesBuilder[ScriptGraphStatement] =
    DseGraphAttributesBuilder(
      DseGraphAttributes(
        tag,
        GraphBoundStatement(
          builder,
          paramNames.map(key => key -> key).toMap))
  )

  /**
    * For backwards compatibility
    *
    * @param paramNamesAndOverrides a Map of Session parameter names to their GraphStatement parameter names
    * @return
    */
  @deprecated("Replaced with withParams")
  def withSetParams(paramNamesAndOverrides: Map[String, String]): DseGraphAttributesBuilder[ScriptGraphStatement] =
    withParams(paramNamesAndOverrides)


  /**
    * Get the parameters mapped to the keys of paramNamesAndOverrides and bind them using the corresponding values of
    * paramNamesAndOverrides to the GraphStatement
    *
    * @param paramNamesAndOverrides a Map of Session parameter names to their GraphStatement parameter names
    * @return
    */
  @deprecated("Replaced with withParams")
  def withParamOverrides(paramNamesAndOverrides: Map[String, String]): DseGraphAttributesBuilder[ScriptGraphStatement] =
    withParams(paramNamesAndOverrides)

  /**
    * Get the parameters mapped to the keys of paramNamesAndOverrides and bind them using the corresponding values of
    * paramNamesAndOverrides to the GraphStatement
    *
    * @param paramNamesAndOverrides a Map of Session parameter names to their GraphStatement parameter names
    * @return
    */
  def withParams(paramNamesAndOverrides: Map[String, String]): DseGraphAttributesBuilder[ScriptGraphStatement] = {
    DseGraphAttributesBuilder(
      DseGraphAttributes(
        tag,
        GraphBoundStatement(builder, paramNamesAndOverrides)))
  }

  /**
    * Repeat the parameters given by suffixing their names and overridden names by a number picked from 1 to batchSize.
    * See http://gatling.io/docs/2.2.2/session/feeder.html to view how multiple values can be picked from feeders
    *
    * @param batchSize              the number of times the parameters should be repeated
    * @param paramNamesAndOverrides a Map of Session parameter names to their GraphStatement parameter names
    * @return
    */
  @deprecated("Replaced by withRepeatedParams")
  def withRepeatedSetParams(batchSize: Int, paramNamesAndOverrides: Map[String, String]): DseGraphAttributesBuilder[ScriptGraphStatement] =
    withRepeatedParams(batchSize, paramNamesAndOverrides)

  /**
    * Repeat the parameters given by suffixing their names and overridden names by a number picked from 1 to batchSize.
    * See http://gatling.io/docs/2.2.2/session/feeder.html to view how multiple values can be picked from feeders
    *
    * @param batchSize              the number of times the parameters should be repeated
    * @param paramNamesAndOverrides a Map of Session parameter names to their GraphStatement parameter names
    * @return
    */
  def withRepeatedParams(batchSize: Int, paramNamesAndOverrides: Map[String, String]): DseGraphAttributesBuilder[ScriptGraphStatement] = {
    def repeatParameters(params: Map[String, String]): Map[String, String] = batchSize match {
      // Gatling has a weird behavior when feeding multiple values
      // Feeding 1 value gives non-suffixed variables whereas feeding more gives suffixed variables starting by the
      // very first one
      case 1 => params
      case x => (
        for (i <- 1 to x) yield
          params.map { case (key, value) => (s"$key$i", s"$value$i") }
        ).flatten.toMap
    }

    DseGraphAttributesBuilder(
      DseGraphAttributes(
        tag,
        GraphBoundStatement(
          builder,
          repeatParameters(paramNamesAndOverrides)))
    )
  }
}
