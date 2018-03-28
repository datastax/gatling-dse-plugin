package com.datastax.gatling.plugin.request

import com.datastax.driver.core.{PreparedStatement, SimpleStatement}
import com.datastax.driver.dse.graph.{GraphStatement, SimpleGraphStatement}
import com.datastax.gatling.plugin._
import com.datastax.gatling.plugin.utils.CqlPreparedStatementUtil
import io.gatling.core.session.{Expression, Session}


/**
  * CQL Request Builder
  *
  * @param tag Name of the CQL Execution
  */
case class CqlRequestBuilder(tag: String) {

  /**
    * Execute String Statement
    *
    * @param query Simple string query
    * @return
    */
  def executeCql(query: Expression[String]) = {
    DseCqlRequestAttributes(DseAttributes(tag, DseCqlStringStatement(query)))
  }

  /**
    * Execute Prepared Statement
    *
    * @param preparedStatement CQL Prepared Statement w/ anon ?'s
    * @return
    */
  def executePrepared(preparedStatement: PreparedStatement) = CqlPreparedStatementBuilder(tag, preparedStatement)

  /**
    * Allows the execution of a prepared statement that has named parameter placeholders - 'select * from tablex where key = :key'
    * Doing this then allows you omit the parameter names and types as they can be inferred from the statement itself
    *
    * @param preparedStatement CQL Prepared statement with :name
    * @return
    */
  def executeNamed(preparedStatement: PreparedStatement) = {
    DseCqlRequestAttributes(DseAttributes(tag, DseCqlBoundStatementNamed(CqlPreparedStatementUtil, preparedStatement),
      cqlStatements = Seq(preparedStatement.getQueryString))
    )
  }

  /**
    * Execute a batch of prepared statements that will auto-infer the values to set based on like names in the Gatling session
    *
    * @param preparedStatements Array of prepared statements
    * @return
    */
  def executePreparedBatch(preparedStatements: Array[PreparedStatement]) = DseCqlRequestAttributes(
    DseAttributes(tag, DseCqlBoundBatchStatement(CqlPreparedStatementUtil, preparedStatements),
      cqlStatements = preparedStatements.map(_.getQueryString)
    )
  )

  /**
    * Execute a Simple Statement
    *
    * @param statement SimpleStatement
    * @return
    */
  def executeStatement(statement: SimpleStatement) = {
    DseCqlRequestAttributes(DseAttributes(tag, DseCqlSimpleStatement(statement),
      cqlStatements = Seq(statement.getQueryString))
    )
  }

  /**
    * Execute a custom Payload
    *
    * @param statement         CQL SimpleStatement
    * @param payloadSessionKey Session key of the payload from session/feed
    * @return
    */
  def executeCustomPayload(statement: SimpleStatement, payloadSessionKey: String) = {
    DseCqlRequestAttributes(DseAttributes(tag, DseCqlCustomPayloadStatement(statement, payloadSessionKey)))
  }

  def executePreparedFromSession(key: String) = {
    DseCqlRequestAttributes(DseAttributes(tag, DseCqlBoundStatementNamedFromSession(CqlPreparedStatementUtil, key)))
  }
}


/**
  * Bind CQL Params to anon query
  *
  * @param tag      Name of the CQL Execution
  * @param prepared CQL Prepared Statement
  */
case class CqlPreparedStatementBuilder(tag: String, prepared: PreparedStatement) {

  /**
    * Alias for the behavior of executeNamed function
    *
    * @return
    */
  def withSessionParams() = {
    DseCqlRequestAttributes(DseAttributes(tag, DseCqlBoundStatementNamed(CqlPreparedStatementUtil, prepared),
      cqlStatements = Seq(prepared.getQueryString))
    )
  }

  /**
    * Bind Gatling Session Values to CQL Prepared Statement
    *
    * @deprecated
    *
    * @param params Gatling Session variables
    * @return
    */
  def withParams(params: Expression[AnyRef]*) = {
    DseCqlRequestAttributes(
      DseAttributes(tag, DseCqlBoundStatementWithPassedParams(CqlPreparedStatementUtil, prepared, params: _*),
        cqlStatements = Seq(prepared.getQueryString))
    )
  }

  /**
    * Bind Gatling Session Keys to CQL Prepared Statement
    *
    * @param sessionKeys Gatling Session Keys
    * @return
    */
  def withParams(sessionKeys: List[String]) = {
    DseCqlRequestAttributes(
      DseAttributes(tag, DseCqlBoundStatementWithParamList(CqlPreparedStatementUtil, prepared, sessionKeys),
        cqlStatements = Seq(prepared.getQueryString))
    )
  }
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
    DseGraphRequestAttributes(DseAttributes(tag, GraphStringStatement(strStatement)))
  }

  /**
    * Execute a Simple Graph Statement, which can include named params
    *
    * @see DseGraphRequestParamsBuilder#withSetParams
    *
    * @param gStatement Simple Graph Statement
    * @return
    */
  def executeGraphStatement(gStatement: SimpleGraphStatement) = {
    DseGraphRequestParamsBuilder(tag, gStatement)
  }

  /**
    * Execute a Fluent Graph API
    *
    * @param gStatement Graph Statement from a Fluent API builder
    * @return
    */
  def executeGraphFluent(gStatement: GraphStatement) = {
    DseGraphRequestAttributes(DseAttributes(tag, GraphFluentStatement(gStatement)))
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
    DseGraphRequestAttributes(DseAttributes(tag, GraphFluentStatementFromScalaLambda(gLambda)))
  }

  /**
    * Execute a traversal previously created by a feeder
    * @param feederKey name of the traversal in the gatling session
    * @return
    */
  def executeGraphFeederTraversal(feederKey: String) = {
    DseGraphRequestAttributes(DseAttributes(tag, GraphFluentSessionKey(feederKey)))
  }
}

/**
  * Param Builder for Simple Graph Statements
  *
  * @param tag        Query tag
  * @param gStatement Simple Graph Staetment
  */
case class DseGraphRequestParamsBuilder(tag: String, gStatement: SimpleGraphStatement) {

  /**
    * Included for compatibility
    *
    * @param paramNames Array of Params names
    * @return
    */
  def withSetParams(paramNames: Array[String]): DseGraphRequestAttributes = withParams(paramNames: _*)

  /**
    * Params to set from strings
    *
    * @param paramNames List of strings to use
    * @return
    */
  def withParams(paramNames: String*): DseGraphRequestAttributes = DseGraphRequestAttributes(
    DseAttributes(tag, GraphBoundStatement(gStatement, paramNames.map(key => key -> key).toMap))
  )


  /**
    * For backwards compatibility
    *
    * @param paramNamesAndOverrides a Map of Session parameter names to their GraphStatement parameter names
    * @return
    */
  def withSetParams(paramNamesAndOverrides: Map[String, String]) = withParamOverrides(paramNamesAndOverrides)


  /**
    * Get the parameters mapped to the keys of paramNamesAndOverrides and bind them using the corresponding values of
    * paramNamesAndOverrides to the GraphStatement
    *
    * @param paramNamesAndOverrides a Map of Session parameter names to their GraphStatement parameter names
    * @return
    */
  def withParamOverrides(paramNamesAndOverrides: Map[String, String]) = {
    DseGraphRequestAttributes(DseAttributes(tag, GraphBoundStatement(gStatement, paramNamesAndOverrides)))
  }

  /**
    * Repeat the parameters given by suffixing their names and overridden names by a number picked from 1 to batchSize.
    * See http://gatling.io/docs/2.2.2/session/feeder.html to view how multiple values can be picked from feeders
    *
    * @param batchSize              the number of times the parameters should be repeated
    * @param paramNamesAndOverrides a Map of Session parameter names to their GraphStatement parameter names
    * @return
    */
  def withRepeatedSetParams(batchSize: Int, paramNamesAndOverrides: Map[String, String]) = {
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
  def withRepeatedParams(batchSize: Int, paramNamesAndOverrides: Map[String, String]) = {
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

    DseGraphRequestAttributes(
      DseAttributes(tag, GraphBoundStatement(gStatement, repeatParameters(paramNamesAndOverrides)))
    )
  }


  //  def withOption(key: String, value: String) = DseGraphRequestParamsBuilder(
  //    tag, gStatement.setGraphInternalOption(key, value).asInstanceOf[SimpleGraphStatement])
}
