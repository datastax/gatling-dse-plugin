package com.datastax.gatling.plugin.model

import com.datastax.driver.core.{PreparedStatement, SimpleStatement}
import com.datastax.gatling.plugin._
import com.datastax.gatling.plugin.utils.CqlPreparedStatementUtil
import io.gatling.core.session.Expression

/**
  * This class is used with the `cql(String)` method in [[DsePredefBase]] to
  * allow the use to type `cql("my-request).executeNamed()` and similar
  * statements.
  *
  * It contains methods that results in Gatling sending CQL queries.
  *
  * @param tag Name of the CQL query to execute
  */
case class DseCqlStatementBuilder(tag: String) {

  /**
    * Execute a simple Statement built from a CQL string.
    *
    * @param query Simple string query
    * @return
    */
  @deprecated("Replaced by executeStatement(String)")
  def executeCql(query: String): DseCqlAttributesBuilder =
    executeStatement(query)

  /**
    * Execute a simple Statement built from a CQL string.
    *
    * @param query Simple string query
    * @return
    */
  def executeStatement(query: String): DseCqlAttributesBuilder =
    executeStatement(new SimpleStatement(query))

  /**
    * Execute a Simple Statement
    *
    * @param statement SimpleStatement
    * @return
    */
  def executeStatement(statement: SimpleStatement): DseCqlAttributesBuilder =
    DseCqlAttributesBuilder(
      DseCqlAttributes(
        tag,
        DseCqlSimpleStatement(statement),
        cqlStatements = Seq(statement.getQueryString))
    )

  /**
    * Execute a prepared Statement.
    *
    * This method is not enough to create a complete [[DseCqlAttributesBuilder]] as
    * the list of parameters must still be defined by the users.
    *
    * @param preparedStatement CQL Prepared Statement w/ anon ?'s
    * @return
    */
  @deprecated("Replaced by executeStatement(PreparedStatement)")
  def executePrepared(preparedStatement: PreparedStatement): DsePreparedCqlStatementBuilder =
    executeStatement(preparedStatement)

  /**
    * Execute a prepared Statement.
    *
    * This method is not enough to create a complete [[DseCqlAttributesBuilder]] as
    * the list of parameters must still be defined by the users, in opposition
    * to [[executeNamed()]].
    *
    * @param preparedStatement CQL Prepared Statement w/ anon ?'s
    * @return
    */
  def executeStatement(preparedStatement: PreparedStatement) =
    DsePreparedCqlStatementBuilder(tag, preparedStatement)

  /**
    * Execute a prepared statement that has named parameter placeholders, for
    * instance 'select * from tablex where key = :key'.
    *
    * Doing this then allows you omit the parameter names and types as they can
    * be inferred from the statement itself.
    *
    * @param preparedStatement CQL Prepared statement with named parameters
    */
  def executeNamed(preparedStatement: PreparedStatement): DseCqlAttributesBuilder =
    DsePreparedCqlStatementBuilder(tag, preparedStatement).withSessionParams()

  /**
    * Execute a batch of prepared statements that have named parameters.
    *
    * @param preparedStatements Array of prepared statements
    */
  def executePreparedBatch(preparedStatements: Array[PreparedStatement]) = DseCqlAttributesBuilder(
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
  def executeCustomPayload(statement: SimpleStatement, payloadSessionKey: String): DseCqlAttributesBuilder =
    DseCqlAttributesBuilder(
      DseCqlAttributes(
        tag,
        DseCqlCustomPayloadStatement(statement, payloadSessionKey),
        cqlStatements = Seq(statement.getQueryString)))

  def executePreparedFromSession(key: String): DseCqlAttributesBuilder =
    DseCqlAttributesBuilder(
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
case class DsePreparedCqlStatementBuilder(tag: String, prepared: PreparedStatement) {

  /**
    * Alias for the behavior of executeNamed function
    *
    * @return
    */
  def withSessionParams(): DseCqlAttributesBuilder =
    DseCqlAttributesBuilder(
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
  def withParams(params: Expression[AnyRef]*): DseCqlAttributesBuilder =
    DseCqlAttributesBuilder(
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
    DseCqlAttributesBuilder(
      DseCqlAttributes(
        tag,
        DseCqlBoundStatementWithParamList(CqlPreparedStatementUtil, prepared, sessionKeys),
        cqlStatements = Seq(prepared.getQueryString))
    )
}
