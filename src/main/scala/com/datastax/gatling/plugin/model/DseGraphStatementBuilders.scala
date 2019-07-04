/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.model

import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement
import com.datastax.dse.graph.api._
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
  def executeGraph(strStatement: Expression[String]) = {
    DseGraphAttributesBuilder(DseGraphAttributes(tag, GraphStringStatement(strStatement)))
  }

  /**
    * Execute a Simple Graph Statement, which can include named params
    *
    * @see DseGraphRequestParamsBuilder#withSetParams
    * @param gStatement Simple Graph Statement
    * @return
    */
  @deprecated("Replaced by executeGraph(SimpleGraphStatement)")
  def executeGraphStatement(gStatement: ScriptGraphStatement) =
    executeGraph(gStatement)

  /**
    * Execute a Simple Graph Statement, which can include named params
    *
    * @see DseGraphRequestParamsBuilder#withSetParams
    * @param gStatement Simple Graph Statement
    * @return
    */
  def executeGraph(gStatement: SimpleGraphStatement) = {
    DseGraphParametrizedStatementBuilder(tag, gStatement)
  }

  /**
    * Execute a Fluent Graph API
    *
    * @param gStatement Graph Statement from a Fluent API builder
    * @return
    */
  def executeGraphFluent(gStatement: GraphStatement) = {
    DseGraphAttributesBuilder(DseGraphAttributes(tag, GraphFluentStatement(gStatement)))
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
  def executeGraphFluent(gLambda: Session => GraphStatement) = {
    DseGraphAttributesBuilder(DseGraphAttributes(tag, GraphFluentStatementFromScalaLambda(gLambda)))
  }

  /**
    * Execute a traversal previously created by a feeder
    *
    * @param feederKey name of the traversal in the gatling session
    * @return
    */
  @deprecated("Replaced by executeGraphFluent{session => session(feederKey)}")
  def executeGraphFeederTraversal(feederKey: String): DseGraphAttributesBuilder = {
    DseGraphAttributesBuilder(DseGraphAttributes(tag, GraphFluentSessionKey(feederKey)))
  }
}

/**
  * Builder for Graph queries that do not have bound parameters yet.
  *
  * @param tag        Query tag
  * @param gStatement Simple Graph Staetment
  */
case class DseGraphParametrizedStatementBuilder(tag: String, gStatement: SimpleGraphStatement) {

  /**
    * Included for compatibility
    *
    * @param paramNames Array of Params names
    * @return
    */
  @deprecated("Replaced by withParams")
  def withSetParams(paramNames: Array[String]): DseGraphAttributesBuilder = withParams(paramNames.toList)

  /**
    * Params to set from strings
    *
    * @param paramNames List of strings to use
    * @return
    */
  def withParams(paramNames: String*): DseGraphAttributesBuilder =
    withParams(paramNames.toList)

  /**
    * Params to set from strings
    *
    * @param paramNames List of strings to use
    * @return
    */
  def withParams(paramNames: List[String]): DseGraphAttributesBuilder = DseGraphAttributesBuilder(
    DseGraphAttributes(tag, GraphBoundStatement(gStatement, paramNames.map(key => key -> key).toMap))
  )

  /**
    * For backwards compatibility
    *
    * @param paramNamesAndOverrides a Map of Session parameter names to their GraphStatement parameter names
    * @return
    */
  @deprecated("Replaced with withParams")
  def withSetParams(paramNamesAndOverrides: Map[String, String]): DseGraphAttributesBuilder =
    withParams(paramNamesAndOverrides)


  /**
    * Get the parameters mapped to the keys of paramNamesAndOverrides and bind them using the corresponding values of
    * paramNamesAndOverrides to the GraphStatement
    *
    * @param paramNamesAndOverrides a Map of Session parameter names to their GraphStatement parameter names
    * @return
    */
  @deprecated("Replaced with withParams")
  def withParamOverrides(paramNamesAndOverrides: Map[String, String]): DseGraphAttributesBuilder =
    withParams(paramNamesAndOverrides)

  /**
    * Get the parameters mapped to the keys of paramNamesAndOverrides and bind them using the corresponding values of
    * paramNamesAndOverrides to the GraphStatement
    *
    * @param paramNamesAndOverrides a Map of Session parameter names to their GraphStatement parameter names
    * @return
    */
  def withParams(paramNamesAndOverrides: Map[String, String]): DseGraphAttributesBuilder = {
    DseGraphAttributesBuilder(DseGraphAttributes(tag, GraphBoundStatement(gStatement, paramNamesAndOverrides)))
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
  def withRepeatedSetParams(batchSize: Int, paramNamesAndOverrides: Map[String, String]): DseGraphAttributesBuilder = {
    withRepeatedParams(batchSize, paramNamesAndOverrides)
  }

  /**
    * Repeat the parameters given by suffixing their names and overridden names by a number picked from 1 to batchSize.
    * See http://gatling.io/docs/2.2.2/session/feeder.html to view how multiple values can be picked from feeders
    *
    * @param batchSize              the number of times the parameters should be repeated
    * @param paramNamesAndOverrides a Map of Session parameter names to their GraphStatement parameter names
    * @return
    */
  def withRepeatedParams(batchSize: Int, paramNamesAndOverrides: Map[String, String]): DseGraphAttributesBuilder = {
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
      DseGraphAttributes(tag, GraphBoundStatement(gStatement, repeatParameters(paramNamesAndOverrides)))
    )
  }
}
