/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin

import java.nio.ByteBuffer

import com.datastax.driver.core._
import com.datastax.gatling.plugin.exceptions.DseCqlStatementException
import com.datastax.gatling.plugin.utils.CqlPreparedStatementUtil
import io.gatling.commons.validation._
import io.gatling.core.session.{Session, _}

import scala.collection.JavaConverters._
import scala.util.{Try, Failure => TryFailure, Success => TrySuccess}


trait DseCqlStatement extends DseStatement[Statement] {
  def buildFromFeeders(session: Session): Validation[Statement]
}


/**
  * Simple CQL Statement from a String
  *
  * @param string the CQL Query to execute
  */
case class DseCqlStringStatement(string: Expression[String]) extends DseCqlStatement {
  def buildFromFeeders(gatlingSession: Session): Validation[SimpleStatement] = {
    string(gatlingSession).flatMap(stmt => new SimpleStatement(stmt).success)
  }
}

/**
  * Simple CQL Statement from the java driver
  *
  * @param statement the statement to execute
  */
case class DseCqlSimpleStatement(statement: SimpleStatement) extends DseCqlStatement {
  def buildFromFeeders(gatlingSession: Session): Validation[SimpleStatement] = {
    statement.success
  }
}

/**
  * Bound CQL Prepared Statement from named parameters
  *
  * @param preparedStatement the prepared statement on which to bind parameters
  */
case class DseCqlBoundStatementNamed(cqlTypes: CqlPreparedStatementUtil, preparedStatement: PreparedStatement)
    extends DseCqlStatement {

  def buildFromFeeders(gatlingSession: Session): Validation[BoundStatement] = {
    if (!cqlTypes.checkIsValidPreparedStatement(preparedStatement)) {
      throw new DseCqlStatementException("Prepared Statements must have at least one settable param. " +
          s"Query: ${preparedStatement.getQueryString}")
    }

    bindParams(gatlingSession, preparedStatement.bind(), cqlTypes.getParamsMap(preparedStatement)).success
  }

  /**
    * Bind Gatling Session Params to CQL Statement by Name and Type
    *
    * @param gatlingSession Gatling Session
    * @param boundStatement CQL Prepared Statement
    * @param queryParams    CQL Query Named Params and Types
    * @return
    */
  protected def bindParams(gatlingSession: Session, boundStatement: BoundStatement,
                           queryParams: Map[String, DataType.Name]): BoundStatement = {

    queryParams.foreach {
      case (gatlingSessionKey, valType) =>
        cqlTypes.bindParamByName(gatlingSession, boundStatement, valType, gatlingSessionKey)
    }
    boundStatement
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
                                                params: Expression[AnyRef]*) extends DseCqlStatement {

  def buildFromFeeders(gatlingSession: Session): Validation[BoundStatement] = {

    if (!cqlTypes.checkIsValidPreparedStatement(preparedStatement)) {
      throw new DseCqlStatementException("Prepared Statements must have at least one settable param. " +
          s"Query: ${preparedStatement.getQueryString}")
    }

    val parsedParams: Seq[Validation[AnyRef]] = params.map(param => param(gatlingSession))
    val (validParsedParams, failures) = parsedParams.partition {
      case Success(_) => true
      case _ => false
    }

    //If we got a failure then fail fast
    failures.headOption match {
      case Some(Failure(error)) => error.failure
      case _ =>
        preparedStatement.bind(validParsedParams.map(_.get): _*).success
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
                                             sessionKeys: Seq[String]) extends DseCqlStatement {

  /**
    * Apply the Gatling session params to the Prepared statement
    *
    * @param gatlingSession DseSession
    * @return
    */
  def buildFromFeeders(gatlingSession: Session): Validation[BoundStatement] = {

    if (!cqlTypes.checkIsValidPreparedStatement(preparedStatement)) {
      throw new DseCqlStatementException("Prepared Statements must have at least one settable param. " +
          s"Query: ${preparedStatement.getQueryString}")
    }

    if (sessionKeys.isEmpty) {
      throw new DseCqlStatementException("Gatling session key list cannot be empty")
    }

    bindParams(gatlingSession, preparedStatement.bind(), sessionKeys).success
  }

  /**
    * Bind the Gatling session params to the CQL Prepared Statement
    *
    * @param gatlingSession Gatling Session
    * @param boundStatement CQL Prepared Statement
    * @param sessionKeys    List of session params to apply, put in order of query ?'s
    * @return
    */
  protected def bindParams(gatlingSession: Session, boundStatement: BoundStatement,
                           sessionKeys: Seq[String]): BoundStatement = {

    val params = cqlTypes.getParamsList(preparedStatement)
    var cnt = 0

    sessionKeys.foreach { gatlingSessionKey =>
      cqlTypes.bindParamByOrder(gatlingSession, boundStatement, params(cnt), gatlingSessionKey, cnt)
      cnt += 1
    }

    boundStatement
  }

}


/**
  * Bound CQL Prepared Statement from Named Params
  *
  * @param statements CQL Prepared Statements
  */
case class DseCqlBoundBatchStatement(cqlTypes: CqlPreparedStatementUtil, statements: Seq[PreparedStatement])
    extends DseCqlStatement {

  def buildFromFeeders(gatlingSession: Session): Validation[BatchStatement] = {

    val batch = new BatchStatement()

    statements.foreach { s =>

      if (!cqlTypes.checkIsValidPreparedStatement(s)) {
        throw new DseCqlStatementException("Prepared Statements must have at least one settable param. " +
            s"Query: ${s.getQueryString}")
      }

      batch.add(bindParams(gatlingSession, s, cqlTypes.getParamsMap(s)))
    }

    batch.success
  }


  /**
    * Bind Gatling Session Params to CQL Statement by Name and Type
    *
    * @param gatlingSession Gatling Session
    * @param statement      CQL Prepared Statement
    * @param queryParams    CQL Query Named Params and Types
    * @return
    */
  protected def bindParams(gatlingSession: Session, statement: PreparedStatement,
                           queryParams: Map[String, DataType.Name]): BoundStatement = {

    val boundStatement = statement.bind()

    if (queryParams.nonEmpty) {
      queryParams.foreach {
        case (gatlingSessionKey, valType) =>
          cqlTypes.bindParamByName(gatlingSession, boundStatement, valType, gatlingSessionKey)
      }
    }
    boundStatement
  }
}


/**
  * Set a custom payload on the statement
  *
  * @param statement  SimpleStaten
  * @param payloadRef session variable for custom payload
  */
case class DseCqlCustomPayloadStatement(statement: SimpleStatement, payloadRef: String) extends DseCqlStatement {

  def buildFromFeeders(gatlingSession: Session): Validation[Statement] = {

    if (!gatlingSession.contains(payloadRef)) {
      throw new DseCqlStatementException(s"Passed sessionKey: {$payloadRef} does not exist in Session.")
    }

    Try {
      val payload = gatlingSession(payloadRef).as[Map[String, ByteBuffer]].asJava
      statement.setOutgoingPayload(payload)
    } match {
      case TrySuccess(stmt) => stmt.success
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
case class DseCqlBoundStatementNamedFromSession(cqlTypes: CqlPreparedStatementUtil, sessionKey: String) extends DseCqlStatement {

  def buildFromFeeders(gatlingSession: Session): Validation[BoundStatement] = {
    if (!gatlingSession.contains(sessionKey)) {
      throw new DseCqlStatementException(s"Passed sessionKey: {$sessionKey} does not exist in Session.")
    }
    val preparedStatement = gatlingSession(sessionKey).as[PreparedStatement]
    DseCqlBoundStatementNamed(cqlTypes, preparedStatement).buildFromFeeders(gatlingSession)
  }
}
