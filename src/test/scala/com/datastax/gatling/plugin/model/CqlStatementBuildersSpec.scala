package com.datastax.gatling.plugin.model

import java.nio.ByteBuffer
import java.time.Duration

import com.datastax.gatling.plugin.DsePredef._
import com.datastax.gatling.plugin.checks.CqlChecks
import com.datastax.oss.driver.api.core.ConsistencyLevel.{EACH_QUORUM, THREE}
import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, SimpleStatement, SimpleStatementBuilder}
import com.datastax.oss.driver.api.core.metadata.Node
import com.datastax.oss.driver.api.core.metadata.token.Token
import io.gatling.core.session.{ExpressionSuccessWrapper, Session}
import org.scalatest.easymock.EasyMockSugar
import org.scalatest.{FlatSpec, Matchers}

class CqlStatementBuildersSpec extends FlatSpec with Matchers with EasyMockSugar {
  behavior of "Builders that produce executable CQL statements"


  it should "build statements from a CQL String" in {
    val statementAttributes: DseCqlAttributes[SimpleStatement,SimpleStatementBuilder] = cql("the-tag")
      .executeStatement("SELECT foo FROM bar.baz LIMIT 1")
      .build()
      .dseAttributes
    val statement = statementAttributes.statement
      .buildFromSession(Session("the-tag", 42))
      .get.build
    statementAttributes.cqlStatements should contain only "SELECT foo FROM bar.baz LIMIT 1"
    statement.getQuery should be("SELECT foo FROM bar.baz LIMIT 1")
  }

  it should "forward all attributs to DseCqlAttributes" in {
    val node = mock[Node]
    val userOrRole = "userOrRole"
    val customPayloadKey = "key"
    val customPayloadVal = mock[ByteBuffer]
    val pagingState = mock[ByteBuffer]
    val queryTimestamp = 123L
    val routingKey = mock[ByteBuffer]
    val routingKeyspace = "some_keyspace"
    val routingToken = mock[Token]
    val timeout = Duration.ofHours(1)
    val cqlCheck = CqlChecks.resultSet.find.is(mock[AsyncResultSet].expressionSuccess).build
    val statementAttributes: DseCqlAttributes[_,_] = cql("the-session-tag")
      .executeStatement("FOO")
      .withConsistencyLevel(EACH_QUORUM)
      .addCustomPayload(customPayloadKey, customPayloadVal)
      .withIdempotency()
      .withNode(node)
      .executeAs(userOrRole)
      .withTracingEnabled()
      .withPageSize(3)
      .withPagingState(pagingState)
      .withQueryTimestamp(queryTimestamp)
      .withRoutingKey(routingKey)
      .withRoutingKeyspace(routingKeyspace)
      .withRoutingToken(routingToken)
      .withSerialConsistencyLevel(THREE)
      .withTimeout(timeout)
      .check(cqlCheck)
      .build()
      .dseAttributes
    statementAttributes.tag should be("the-session-tag")
    statementAttributes.cl should be(Some(EACH_QUORUM))
    statementAttributes.cqlChecks should contain only cqlCheck
    statementAttributes.customPayload should be(Some(Map(customPayloadKey -> customPayloadVal)))
    statementAttributes.idempotent should be(Some(true))
    statementAttributes.node should be(Some(node))
    statementAttributes.userOrRole should be(Some(userOrRole))
    statementAttributes.enableTrace should be(Some(true))
    statementAttributes.pageSize should be(Some(3))
    statementAttributes.pagingState should be(Some(pagingState))
    statementAttributes.queryTimestamp should be(Some(queryTimestamp))
    statementAttributes.routingKey should be(Some(routingKey))
    statementAttributes.routingKeyspace should be(Some(routingKeyspace))
    statementAttributes.routingToken should be(Some(routingToken))
    statementAttributes.serialCl should be(Some(THREE))
    statementAttributes.timeout should be(Some(timeout))
    statementAttributes.cqlStatements should contain only "FOO"
  }

  it should "build statements from a SimpleStatement" in {
    val statementAttributes: DseCqlAttributes[SimpleStatement,SimpleStatementBuilder] = cql("the-tag")
      .executeStatement(SimpleStatement.newInstance("Some CQL"))
      .build()
      .dseAttributes
    val statement = statementAttributes.statement
      .buildFromSession(Session("the-tag", 42))
      .get.build
    statementAttributes.cqlStatements should contain only "Some CQL"
    statement.getQuery should be("Some CQL")
  }

  //  it should "build statements from a PreparedStatement" in {
  //    val preparedStatement = mock[PreparedStatement]
  //    expecting {
  //      preparedStatement.getVariables.andReturn(ColumnDefinitions.EMPTY)
  //    }
  //    val statementAttributes: DseCqlAttributes = cql("the-tag")
  //      .executeStatement(preparedStatement)
  //      .withParams()
  //      .build()
  //      .dseAttributes
  //    val statement: SimpleStatement = statementAttributes.statement
  //      .buildFromFeeders(Session("the-tag", 42))
  //      .get
  //      .asInstanceOf[SimpleStatement]
  //    statementAttributes.cqlStatements should contain only "Some CQL"
  //    statement.getQueryString() should be("Some CQL")
  //  }

}
