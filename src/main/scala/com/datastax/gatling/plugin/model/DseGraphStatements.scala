/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.model

import com.datastax.dse.driver.api.core.graph.{FluentGraphStatement, GraphStatement, ScriptGraphStatement}
import com.datastax.dse.graph.api.DseGraph
import com.datastax.gatling.plugin.exceptions.DseGraphStatementException
import io.gatling.commons.validation._
import io.gatling.core.session.{Expression, Session}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal

import scala.util.{Try, Failure => TryFailure, Success => TrySuccess}


trait DseGraphStatement[T] extends DseStatement[T] {
  def buildFromSession(session: Session): Validation[T]
}

/**
  * Simple DSE Graph Statement from a String
  *
  * @param statement the Gremlin String to execute
  */
case class GraphStringStatement(statement: Expression[String]) extends DseGraphStatement[ScriptGraphStatement] {
  def buildFromSession(gatlingSession: Session): Validation[ScriptGraphStatement] = {
    statement(gatlingSession).flatMap(stmt => ScriptGraphStatement.builder(stmt).build().success)
  }
}

/**
  * DSE Graph Fluent API Statement
  *
  * @param statement the Fluent Statement
  */
case class GraphFluentStatement(statement: FluentGraphStatement) extends DseGraphStatement[FluentGraphStatement] {
  def buildFromSession(gatlingSession: Session): Validation[FluentGraphStatement] = {
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
case class GraphFluentStatementFromScalaLambda(lambda: Session => FluentGraphStatement) extends DseGraphStatement[FluentGraphStatement] {
  def buildFromSession(gatlingSession: Session): Validation[FluentGraphStatement] = {
    lambda(gatlingSession).success
  }
}

/**
  * Graph Fluent API Session Support.
  * Useful when you need to build your traversal based on the value in the Gatling User Session.
  *
  * @param sessionKey Place a GraphTraversal in your session with this key name
  */
case class GraphFluentSessionKey(sessionKey: String) extends DseGraphStatement[FluentGraphStatement] {

  def buildFromSession(gatlingSession: Session): Validation[FluentGraphStatement] = {

    if (!gatlingSession.contains(sessionKey)) {
      throw new DseGraphStatementException(s"Passed sessionKey: {$sessionKey} does not exist in Session.")
    }

    Try {
      FluentGraphStatement.newInstance(gatlingSession(sessionKey).as[GraphTraversal[_, _]])
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
case class GraphBoundStatement(statement: ScriptGraphStatement, sessionKeys: Map[String, String]) extends DseGraphStatement[ScriptGraphStatement] {

  /**
    * Apply the Gatling session params passed to the GraphStatement
    *
    * @param gatlingSession Gatling Session
    * @return
    */
  def buildFromSession(gatlingSession: Session): Validation[ScriptGraphStatement] = {
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
  private def setParam(gatlingSession: Session, paramName: String, overriddenParamName: String): ScriptGraphStatement = {
    statement.setQueryParam(overriddenParamName, gatlingSession(paramName).as[Object])
  }
}