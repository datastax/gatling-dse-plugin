/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.request

import com.datastax.driver.core.{PreparedStatement, SimpleStatement}
import com.datastax.driver.dse.graph.{GraphStatement, SimpleGraphStatement}
import com.datastax.gatling.plugin._
import com.datastax.gatling.plugin.utils.CqlPreparedStatementUtil
import io.gatling.core.session.{Expression, Session}

/**
  * This class is used with the `cql(String)` method in [[DsePredefBase]] to
  * allow the use to type `cql("my-request).executeNamed()` and similar
  * statements.
  *
  * It contains methods that results in Gatling sending CQL queries.
  *
  * @param tag Name of the CQL query to execute
  */
case class CqlRequestBuilder(tag: String) {

  /**
    * Execute a simple Statement built from a CQL string.
    *
    * @param query Simple string query
    * @return
    */
  @deprecated("Replaced by executeStatement(String)")
  def executeCql(query: String): DseCqlRequestBuilder =
    executeStatement(query)

  /**
    * Execute a simple Statement built from a CQL string.
    *
    * @param query Simple string query
    * @return
    */
  def executeStatement(query: String): DseCqlRequestBuilder =
    executeStatement(new SimpleStatement(query))

  /**
    * Execute a Simple Statement
    *
    * @param statement SimpleStatement
    * @return
    */
  def executeStatement(statement: SimpleStatement): DseCqlRequestBuilder =
    DseCqlRequestBuilder(
      DseCqlAttributes(
        tag,
        DseCqlSimpleStatement(statement),
        cqlStatements = Seq(statement.getQueryString))
    )

  /**
    * Execute a prepared Statement.
    *
    * This method is not enough to create a complete [[DseCqlRequestBuilder]] as
    * the list of parameters must still be defined by the users.
    *
    * @param preparedStatement CQL Prepared Statement w/ anon ?'s
    * @return
    */
  @deprecated("Replaced by executeStatement(PreparedStatement)")
  def executePrepared(preparedStatement: PreparedStatement): PreparedCqlRequestBuilder =
    executeStatement(preparedStatement)

  /**
    * Execute a prepared Statement.
    *
    * This method is not enough to create a complete [[DseCqlRequestBuilder]] as
    * the list of parameters must still be defined by the users, in opposition
    * to [[executeNamed()]].
    *
    * @param preparedStatement CQL Prepared Statement w/ anon ?'s
    * @return
    */
  def executeStatement(preparedStatement: PreparedStatement) =
    PreparedCqlRequestBuilder(tag, preparedStatement)

  /**
    * Execute a prepared statement that has named parameter placeholders, for
    * instance 'select * from tablex where key = :key'.
    *
    * Doing this then allows you omit the parameter names and types as they can
    * be inferred from the statement itself.
    *
    * @param preparedStatement CQL Prepared statement with named parameters
    */
  def executeNamed(preparedStatement: PreparedStatement): DseCqlRequestBuilder =
    PreparedCqlRequestBuilder(tag, preparedStatement).withSessionParams()

  /**
    * Execute a batch of prepared statements that have named parameters.
    *
    * @param preparedStatements Array of prepared statements
    */
  def executePreparedBatch(preparedStatements: Array[PreparedStatement]) = DseCqlRequestBuilder(
    DseCqlAttributes(
      tag,
      DseCqlBoundBatchStatement(CqlPreparedStatementUtil, preparedStatements),
      cqlStatements = preparedStatements.map(_.getQueryString)
    )
  )

  /**
    * Execute a custom Payload
    *
    * @param statement         CQL SimpleStatement
    * @param payloadSessionKey Session key of the payload from session/feed
    * @return
    */
  def executeCustomPayload(statement: SimpleStatement, payloadSessionKey: String): DseCqlRequestBuilder =
    DseCqlRequestBuilder(
      DseCqlAttributes(
        tag,
        DseCqlCustomPayloadStatement(statement, payloadSessionKey),
        cqlStatements = Seq(statement.getQueryString)))

  def executePreparedFromSession(key: String): DseCqlRequestBuilder =
    DseCqlRequestBuilder(
      DseCqlAttributes(
        tag,
        DseCqlBoundStatementNamedFromSession(CqlPreparedStatementUtil, key)))
}


/**
  * Builder for CQL prepared statements that do not have bound parameters yet.
  *
  * @param tag      Name of the CQL Execution
  * @param prepared CQL Prepared Statement
  */
case class PreparedCqlRequestBuilder(tag: String, prepared: PreparedStatement) {

  /**
    * Alias for the behavior of executeNamed function
    *
    * @return
    */
  def withSessionParams(): DseCqlRequestBuilder =
    DseCqlRequestBuilder(
      DseCqlAttributes(
        tag,
        DseCqlBoundStatementNamed(CqlPreparedStatementUtil, prepared),
        cqlStatements = Seq(prepared.getQueryString)))

  /**
    * Bind Gatling Session Values to CQL Prepared Statement
    *
    * @param params Gatling Session variables
    * @return
    */
  def withParams(params: Expression[AnyRef]*): DseCqlRequestBuilder =
    DseCqlRequestBuilder(
      DseCqlAttributes(
        tag,
        DseCqlBoundStatementWithPassedParams(CqlPreparedStatementUtil, prepared, params: _*),
        cqlStatements = Seq(prepared.getQueryString))
    )

  /**
    * Bind Gatling Session Keys to CQL Prepared Statement
    *
    * @param sessionKeys Gatling Session Keys
    * @return
    */
  def withParams(sessionKeys: List[String]) =
    DseCqlRequestBuilder(
      DseCqlAttributes(
        tag,
        DseCqlBoundStatementWithParamList(CqlPreparedStatementUtil, prepared, sessionKeys),
        cqlStatements = Seq(prepared.getQueryString))
    )
}

/**
  * DSE Graph Request Builder
  *
  * @param tag Graph Query Tag to be included in Reports
  */
case class GraphRequestBuilder(tag: String) {

  /**
    * Execute a single string graph query
    *
    * @param strStatement Graph Query String
    * @return
    */
  def executeGraph(strStatement: Expression[String]) = {
    DseGraphRequestBuilder(DseGraphAttributes(tag, GraphStringStatement(strStatement)))
  }

  /**
    * Execute a Simple Graph Statement, which can include named params
    *
    * @see DseGraphRequestParamsBuilder#withSetParams
    * @param gStatement Simple Graph Statement
    * @return
    */
  @deprecated("Replaced by executeGraph(SimpleGraphStatement)")
  def executeGraphStatement(gStatement: SimpleGraphStatement) =
    executeGraph(gStatement)

  /**
    * Execute a Simple Graph Statement, which can include named params
    *
    * @see DseGraphRequestParamsBuilder#withSetParams
    * @param gStatement Simple Graph Statement
    * @return
    */
  def executeGraph(gStatement: SimpleGraphStatement) = {
    GraphParametrizedRequestBuilder(tag, gStatement)
  }

  /**
    * Execute a Fluent Graph API
    *
    * @param gStatement Graph Statement from a Fluent API builder
    * @return
    */
  def executeGraphFluent(gStatement: GraphStatement) = {
    DseGraphRequestBuilder(DseGraphAttributes(tag, GraphFluentStatement(gStatement)))
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
    DseGraphRequestBuilder(DseGraphAttributes(tag, GraphFluentStatementFromScalaLambda(gLambda)))
  }

  /**
    * Execute a traversal previously created by a feeder
    *
    * @param feederKey name of the traversal in the gatling session
    * @return
    */
  @deprecated("Replaced by executeGraphFluent{session => session(feederKey)}")
  def executeGraphFeederTraversal(feederKey: String): DseGraphRequestBuilder = {
    DseGraphRequestBuilder(DseGraphAttributes(tag, GraphFluentSessionKey(feederKey)))
  }
}

/**
  * Builder for Graph queries that do not have bound parameters yet.
  *
  * @param tag        Query tag
  * @param gStatement Simple Graph Staetment
  */
case class GraphParametrizedRequestBuilder(tag: String, gStatement: SimpleGraphStatement) {

  /**
    * Included for compatibility
    *
    * @param paramNames Array of Params names
    * @return
    */
  @deprecated("Replaced by withParams")
  def withSetParams(paramNames: Array[String]): DseGraphRequestBuilder = withParams(paramNames.toList)

  /**
    * Params to set from strings
    *
    * @param paramNames List of strings to use
    * @return
    */
  def withParams(paramNames: String*): DseGraphRequestBuilder =
    withParams(paramNames.toList)

  /**
    * Params to set from strings
    *
    * @param paramNames List of strings to use
    * @return
    */
  def withParams(paramNames: List[String]): DseGraphRequestBuilder = DseGraphRequestBuilder(
    DseGraphAttributes(tag, GraphBoundStatement(gStatement, paramNames.map(key => key -> key).toMap))
  )

  /**
    * For backwards compatibility
    *
    * @param paramNamesAndOverrides a Map of Session parameter names to their GraphStatement parameter names
    * @return
    */
  @deprecated("Replaced with withParams")
  def withSetParams(paramNamesAndOverrides: Map[String, String]): DseGraphRequestBuilder =
    withParams(paramNamesAndOverrides)


  /**
    * Get the parameters mapped to the keys of paramNamesAndOverrides and bind them using the corresponding values of
    * paramNamesAndOverrides to the GraphStatement
    *
    * @param paramNamesAndOverrides a Map of Session parameter names to their GraphStatement parameter names
    * @return
    */
  @deprecated("Replaced with withParams")
  def withParamOverrides(paramNamesAndOverrides: Map[String, String]): DseGraphRequestBuilder =
    withParams(paramNamesAndOverrides)

  /**
    * Get the parameters mapped to the keys of paramNamesAndOverrides and bind them using the corresponding values of
    * paramNamesAndOverrides to the GraphStatement
    *
    * @param paramNamesAndOverrides a Map of Session parameter names to their GraphStatement parameter names
    * @return
    */
  def withParams(paramNamesAndOverrides: Map[String, String]): DseGraphRequestBuilder = {
    DseGraphRequestBuilder(DseGraphAttributes(tag, GraphBoundStatement(gStatement, paramNamesAndOverrides)))
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
  def withRepeatedSetParams(batchSize: Int, paramNamesAndOverrides: Map[String, String]): DseGraphRequestBuilder = {
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
  def withRepeatedParams(batchSize: Int, paramNamesAndOverrides: Map[String, String]): DseGraphRequestBuilder = {
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

    DseGraphRequestBuilder(
      DseGraphAttributes(tag, GraphBoundStatement(gStatement, repeatParameters(paramNamesAndOverrides)))
    )
  }
}
