/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.model

import com.datastax.dse.driver.api.core.graph._
import com.datastax.gatling.plugin.exceptions.DseGraphStatementException
import io.gatling.commons.validation._
import io.gatling.core.session.{Expression, Session}
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal

import scala.util.{Try, Failure => TryFailure, Success => TrySuccess}

trait DseGraphStatement[T <: GraphStatement[T], B <: GraphStatementBuilderBase[B,T]] extends DseStatement[B]

/**
  * Simple DSE Graph Statement from a String
  *
  * @param statement the Gremlin String to execute
  */
case class GraphStringStatement(statement: Expression[String])
  extends DseGraphStatement[ScriptGraphStatement, ScriptGraphStatementBuilder] {

  def buildFromSession(gatlingSession: Session): Validation[ScriptGraphStatementBuilder] = {
    statement(gatlingSession).flatMap(stmt => ScriptGraphStatement.builder(stmt).success)
  }
}

/**
  * DSE Graph Fluent API Statement
  *
  * @param statement the Fluent Statement
  */
case class GraphFluentStatement(statement: FluentGraphStatement)
  extends DseGraphStatement[FluentGraphStatement, FluentGraphStatementBuilder] {

  def buildFromSession(gatlingSession: Session): Validation[FluentGraphStatementBuilder] = {
    FluentGraphStatement.builder(statement).success
  }
}

/**
  * Graph Fluent API Statement parameter support
  * Useful when you need to build a fluent statement based on parameters, e.g. from a feeder
  *
  * @param lambda Scala lambda that takes a Gatling User Session (from which it can retrieve parameters)
  *               and returns a fluent Graph Statement
  */
case class GraphFluentStatementFromScalaLambda(lambda: Session => FluentGraphStatement)
  extends DseGraphStatement[FluentGraphStatement, FluentGraphStatementBuilder] {

  def buildFromSession(gatlingSession: Session): Validation[FluentGraphStatementBuilder] = {
    FluentGraphStatement.builder(lambda(gatlingSession)).success
  }
}

/**
  * Graph Fluent API Session Support.
  * Useful when you need to build your traversal based on the value in the Gatling User Session.
  *
  * @param sessionKey Place a GraphTraversal in your session with this key name
  */
case class GraphFluentSessionKey(sessionKey: String)
  extends DseGraphStatement[FluentGraphStatement, FluentGraphStatementBuilder] {

  def buildFromSession(gatlingSession: Session): Validation[FluentGraphStatementBuilder] = {

    if (!gatlingSession.contains(sessionKey)) {
      throw new DseGraphStatementException(s"Passed sessionKey: {$sessionKey} does not exist in Session.")
    }

    Try {
      FluentGraphStatement.builder(gatlingSession(sessionKey).as[GraphTraversal[_, _]])
    } match {
      case TrySuccess(builder) => builder.success
      case TryFailure(error) => error.getMessage.failure
    }
  }
}

/**
  * Set/Bind Gatling Session key/vals to GraphStatement
  *
  * @param builder   SimpleGraphStatementBuilder
  * @param sessionKeys Gatling session param keys mapped to their bind name, to allow name override
  */
case class GraphBoundStatement(builder: ScriptGraphStatementBuilder, sessionKeys: Map[String, String])
  extends DseGraphStatement[ScriptGraphStatement, ScriptGraphStatementBuilder] {

  /**
    * Apply the Gatling session params passed to the GraphStatement
    *
    * @param gatlingSession Gatling Session
    * @return
    */

  def buildFromSession(gatlingSession: Session): Validation[ScriptGraphStatementBuilder] = {
    Try {
      sessionKeys foreach { k =>
        k match {
          case (k, v) => builder.setQueryParam(v, gatlingSession(k).as[Object])
          case _ => throw new RuntimeException(s"Observed ${k} instead of expected key-value pair")
        }
      }
      builder
    } match {
      case TrySuccess(builder) => builder.success
      case TryFailure(error) => error.getMessage.failure
    }
  }
}