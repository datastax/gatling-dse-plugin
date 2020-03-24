/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.model

import java.nio.ByteBuffer

import com.datastax.oss.driver.api.core.cql._
import com.datastax.gatling.plugin.exceptions.DseCqlStatementException
import com.datastax.gatling.plugin.utils.CqlPreparedStatementUtil
import com.datastax.oss.driver.api.core.`type`.DataType
import io.gatling.commons.validation._
import io.gatling.core.session._

import scala.collection.JavaConverters._
import scala.util.{Try, Failure => TryFailure, Success => TrySuccess}

trait DseCqlStatement[T <: Statement[T], B <: StatementBuilder[B,T]] extends DseStatement[B]

/**
  * Simple CQL Statement from the java driver
  *
  * @param statement the statement to execute
  */
case class DseCqlSimpleStatement(statement: SimpleStatement)
  extends DseCqlStatement[SimpleStatement, SimpleStatementBuilder] {

  def buildFromSession(gatlingSession: Session): Validation[SimpleStatementBuilder] = {
    SimpleStatement.builder(statement).success
  }
}

/**
  * Bound CQL Prepared Statement from named parameters
  *
  * @param preparedStatement the prepared statement on which to bind parameters
  */
case class DseCqlBoundStatementNamed(cqlTypes: CqlPreparedStatementUtil,
                                     preparedStatement: PreparedStatement)
                                    (implicit builderFn: (BoundStatement) => BoundStatementBuilder)
  extends DseCqlStatement[BoundStatement, BoundStatementBuilder] {

  def buildFromSession(gatlingSession: Session): Validation[BoundStatementBuilder] = {
    val template:BoundStatement = bindParams(
      gatlingSession,
      preparedStatement.bind(),
      cqlTypes.getParamsMap(preparedStatement))
    builderFn(template).success
  }

  /**
    * Bind Gatling Session Params to CQL Statement by Name and Type
    *
    * @param gatlingSession Gatling Session
    * @param template CQL Prepared Statement
    * @param queryParams    CQL Query Named Params and Types
    * @return
    */
  protected def bindParams(gatlingSession: Session, template: BoundStatement,
                           queryParams: Map[String, DataType]): BoundStatement = {
    val completedBuilder =
      queryParams.foldLeft(builderFn(template)) {
        (builder, kv) =>
          kv match {
            case (gatlingSessionKey, valType) =>
              cqlTypes.bindParamByName(gatlingSession, builder, valType, gatlingSessionKey)
          }
      }
    completedBuilder.build()
  }
}

/**
  * Bind Gatling session values to the CQL Prepared Statement
  *
  * @param preparedStatement CQL Prepared Statement
  * @param params            Gatling Session Params
  */
case class DseCqlBoundStatementWithPassedParams(cqlTypes: CqlPreparedStatementUtil,
                                                preparedStatement: PreparedStatement,
                                                params: Expression[AnyRef]*)
                                               (implicit builderFn: (BoundStatement) => BoundStatementBuilder)
  extends DseCqlStatement[BoundStatement, BoundStatementBuilder] {

  def buildFromSession(gatlingSession: Session): Validation[BoundStatementBuilder] = {
    val parsedParams: Seq[Validation[AnyRef]] = params.map(param => param(gatlingSession))
    if (parsedParams.exists(_.isInstanceOf[Failure])) {
      val firstError = StringBuilder.newBuilder
      parsedParams
        .filter(_.isInstanceOf[Failure])
        .head
        .onFailure(msg => firstError.append(msg))
      firstError.toString().failure
    } else {
      val template:BoundStatement = preparedStatement.bind(parsedParams.map(_.get): _*)
      builderFn(template).success
    }
  }
}

/**
  * Bind Gatling session params to the CQL Prepared Statement
  *
  * @param sessionKeys List of session params to bind to the prepared statement
  */
case class DseCqlBoundStatementWithParamList(cqlTypes: CqlPreparedStatementUtil,
                                             preparedStatement: PreparedStatement,
                                             sessionKeys: Seq[String])
                                            (implicit builderFn: (BoundStatement) => BoundStatementBuilder)
  extends DseCqlStatement[BoundStatement, BoundStatementBuilder] {

  /**
    * Apply the Gatling session params to the Prepared statement
    *
    * @param gatlingSession current Gatling session
    * @return
    */
  def buildFromSession(gatlingSession: Session): Validation[BoundStatementBuilder] = {
    val template:BoundStatement = bindParams(gatlingSession, preparedStatement.bind(), sessionKeys)
    builderFn(template).success
  }

  /**
    * Bind the Gatling session params to the CQL Prepared Statement
    *
    * @param gatlingSession Gatling Session
    * @param template CQL Prepared Statement
    * @param sessionKeys    List of session params to apply, put in order of query ?'s
    * @return
    */
  protected def bindParams(gatlingSession: Session, template: BoundStatement,
                           sessionKeys: Seq[String]): BoundStatement = {
    val params = cqlTypes.getParamsList(preparedStatement)
    val completedBuilder =
      sessionKeys.zip(Iterable.range(0,sessionKeys.size)).foldLeft(builderFn(template)) {
        (builder, kv) =>
          kv match {
            case (sessionKey, cnt) => cqlTypes.bindParamByOrder(gatlingSession, builder, params(cnt), sessionKey, cnt)
          }
        }
    completedBuilder.build()
  }
}

/**
  * Bound CQL Prepared Statement from Named Params
  *
  * @param statements CQL Prepared Statements
  */
case class DseCqlBoundBatchStatement(cqlTypes: CqlPreparedStatementUtil,
                                     statements: Seq[PreparedStatement])
                                    (implicit builderFn: (BoundStatement) => BoundStatementBuilder)
  extends DseCqlStatement[BatchStatement, BatchStatementBuilder] {

  def buildFromSession(gatlingSession: Session): Validation[BatchStatementBuilder] = {
    val builder:BatchStatementBuilder = BatchStatement.builder(DefaultBatchType.LOGGED)
    val batchables = statements.map(bindParams(gatlingSession))
    builder.addStatements(batchables:_*).success
  }

  /**
    * Bind Gatling Session Params to CQL Statement by Name and Type
    *
    * @param gatlingSession Gatling Session
    * @param statement      CQL Prepared Statement
    * @return
    */
  def bindParams(gatlingSession: Session)(statement: PreparedStatement): BoundStatement = {
    val queryParams: Map[String, DataType] = cqlTypes.getParamsMap(statement)
    val initBuilder:BoundStatementBuilder = builderFn(statement.bind())
    val completedBuilder =
      queryParams.foldLeft(initBuilder) {
        (builder, kv) =>
          kv match {
            case (gatlingSessionKey, valType) =>
              cqlTypes.bindParamByName(gatlingSession, builder, valType, gatlingSessionKey)
          }
      }
    completedBuilder.build()
  }
}

/**
  * Set a custom payload on the statement
  *
  * @param statement  SimpleStaten
  * @param payloadRef session variable for custom payload
  */
case class DseCqlCustomPayloadStatement(statement: SimpleStatement, payloadRef: String)
  extends DseCqlStatement[SimpleStatement, SimpleStatementBuilder] {

  def buildFromSession(gatlingSession: Session): Validation[SimpleStatementBuilder] = {
    if (!gatlingSession.contains(payloadRef)) {
      throw new DseCqlStatementException(s"Passed sessionKey: {$payloadRef} does not exist in Session.")
    }

    Try {
      val payload = gatlingSession(payloadRef).as[Map[String, ByteBuffer]].asJava
      statement.setCustomPayload(payload)
    } match {
      case TrySuccess(stmt) => SimpleStatement.builder(stmt).success
      case TryFailure(error) => error.getMessage.failure
    }
  }
}

/**
  * Fetch a prepared statement from the Gatling session, given its key and binds all its parameters.
  * The prepared statement MUST have at least one variable.
  *
  * @param sessionKey the session key which is associated to a PreparedStatement
  */
case class DseCqlBoundStatementNamedFromSession(cqlTypes: CqlPreparedStatementUtil,
                                                sessionKey: String)
                                               (implicit builderFn: (BoundStatement) => BoundStatementBuilder)
  extends DseCqlStatement[BoundStatement, BoundStatementBuilder] {

  def buildFromSession(gatlingSession: Session): Validation[BoundStatementBuilder] = {
    if (!gatlingSession.contains(sessionKey)) {
      throw new DseCqlStatementException(s"Passed sessionKey: {$sessionKey} does not exist in Session.")
    }
    val preparedStatement = gatlingSession(sessionKey).as[PreparedStatement]
    DseCqlBoundStatementNamed(cqlTypes, preparedStatement)(builderFn).buildFromSession(gatlingSession)
  }
}
