/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.model

import com.datastax.dse.driver.api.core.graph.{DseGraph, GraphStatement, SimpleGraphStatement}
import com.datastax.gatling.plugin.exceptions.DseGraphStatementException
import io.gatling.commons.validation._
import io.gatling.core.session.{Expression, Session}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal

import scala.util.{Try, Failure => TryFailure, Success => TrySuccess}


trait DseGraphStatement extends DseStatement[GraphStatement] {
  def buildFromSession(session: Session): Validation[GraphStatement]
}

/**
  * Simple DSE Graph Statement from a String
  *
  * @param statement the Gremlin String to execute
  */
case class GraphStringStatement(statement: Expression[String]) extends DseGraphStatement {
  def buildFromSession(gatlingSession: Session): Validation[GraphStatement] = {
    statement(gatlingSession).flatMap(stmt => new SimpleGraphStatement(stmt).success)
  }
}

/**
  * DSE Graph Fluent API Statement
  *
  * @param statement the Fluent Statement
  */
case class GraphFluentStatement(statement: GraphStatement) extends DseGraphStatement {
  def buildFromSession(gatlingSession: Session): Validation[GraphStatement] = {
    statement.success
  }
}

/**
  * Graph Fluent API Statement parameter support
  * Useful when you need to build a fluent statement based on parameters, e.g. from a feeder
  *
  * @param lambda Scala lambda that takes a Gatling User Session (from which it can retrieve parameters)
  *               and returns a fluent Graph Statement
  */
case class GraphFluentStatementFromScalaLambda(lambda: Session => GraphStatement) extends DseGraphStatement {
  def buildFromSession(gatlingSession: Session): Validation[GraphStatement] = {
    lambda(gatlingSession).success
  }
}

/**
  * Graph Fluent API Session Support.
  * Useful when you need to build your traversal based on the value in the Gatling User Session.
  *
  * @param sessionKey Place a GraphTraversal in your session with this key name
  */
case class GraphFluentSessionKey(sessionKey: String) extends DseGraphStatement {

  def buildFromSession(gatlingSession: Session): Validation[GraphStatement] = {

    if (!gatlingSession.contains(sessionKey)) {
      throw new DseGraphStatementException(s"Passed sessionKey: {$sessionKey} does not exist in Session.")
    }

    Try {
      DseGraph.statementFromTraversal(gatlingSession(sessionKey).as[GraphTraversal[_, _]])
    } match {
      case TrySuccess(stmt) => stmt.success
      case TryFailure(error) => error.getMessage.failure
    }
  }

}

/**
  * Set/Bind Gatling Session key/vals to GraphStatement
  *
  * @param statement   SimpleGraphStatement
  * @param sessionKeys Gatling session param keys mapped to their bind name, to allow name override
  */
case class GraphBoundStatement(statement: SimpleGraphStatement, sessionKeys: Map[String, String]) extends DseGraphStatement {

  /**
    * Apply the Gatling session params passed to the GraphStatement
    *
    * @param gatlingSession Gatling Session
    * @return
    */
  def buildFromSession(gatlingSession: Session): Validation[GraphStatement] = {
    Try {
      sessionKeys.foreach((tuple: (String, String)) => setParam(gatlingSession, tuple._1, tuple._2)).success
      statement
    } match {
      case TrySuccess(stmt) => stmt.success
      case TryFailure(error) => error.getMessage.failure
    }
  }

  /**
    * Set a parameter to the current gStatement.
    *
    * The value of this parameter is retrieved by looking up paramName from the gatlingSession.
    * It is then bound to overriddenParamName in gStatement.
    *
    * @param gatlingSession      Gatling session
    * @param paramName           Parameter name as accessible in Gatling session
    * @param overriddenParamName Parameter name used to bind the statement
    * @return gStatement
    */
  private def setParam(gatlingSession: Session, paramName: String, overriddenParamName: String): GraphStatement = {
    statement.set(overriddenParamName, gatlingSession(paramName).as[Object])
  }
}