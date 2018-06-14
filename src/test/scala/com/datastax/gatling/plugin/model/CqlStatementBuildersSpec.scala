package com.datastax.gatling.plugin.model

import com.datastax.driver.core.ConsistencyLevel.{EACH_QUORUM, THREE}
import com.datastax.driver.core._
import com.datastax.driver.core.policies.FallthroughRetryPolicy
import com.datastax.gatling.plugin.DsePredef._
import com.datastax.gatling.plugin.checks.{CqlChecks, DseCqlCheck, GenericCheck, GenericChecks}
import io.gatling.core.Predef._
import io.gatling.core.session.{ExpressionSuccessWrapper, Session}
import org.scalatest.easymock.EasyMockSugar
import org.scalatest.{FlatSpec, Matchers}

class CqlStatementBuildersSpec extends FlatSpec with Matchers with EasyMockSugar {
  behavior of "Builders that produce executable CQL statements"


  it should "build statements from a CQL String" in {
    val statementAttributes: DseCqlAttributes = cql("the-tag")
      .executeCql("SELECT foo FROM bar.baz LIMIT 1")
      .build()
      .dseAttributes
    val statement: SimpleStatement = statementAttributes.statement
      .buildFromFeeders(Session("the-tag", 42))
      .get.asInstanceOf[SimpleStatement]
    statementAttributes.cqlStatements should contain only "SELECT foo FROM bar.baz LIMIT 1"
    statement.getQueryString() should be("SELECT foo FROM bar.baz LIMIT 1")
  }

  it should "forward all attributs to DseCqlAttributes" in {
    val pagingState = mock[PagingState]
    val genericCheck = GenericCheck(GenericChecks.exhausted.is(true))
    val cqlCheck = DseCqlCheck(CqlChecks.oneRow.is(mock[Row].expressionSuccess))
    val statementAttributes: DseCqlAttributes = cql("the-session-tag")
      .executeCql("FOO")
      .withConsistencyLevel(EACH_QUORUM)
      .withUserOrRole("User or role")
      .withDefaultTimestamp(-76)
      .withIdempotency()
      .withReadTimeout(99)
      .withSerialConsistencyLevel(THREE)
      .withRetryPolicy(FallthroughRetryPolicy.INSTANCE)
      .withFetchSize(3)
      .withTracingEnabled()
      .withPagingState(pagingState)
      .check(genericCheck)
      .check(cqlCheck)
      .build()
      .dseAttributes
    statementAttributes.tag should be("the-session-tag")
    statementAttributes.cl should be(Some(EACH_QUORUM))
    statementAttributes.cqlChecks should contain only cqlCheck
    statementAttributes.genericChecks should contain only genericCheck
    statementAttributes.userOrRole should be(Some("User or role"))
    statementAttributes.readTimeout should be(Some(99))
    statementAttributes.idempotent should be(Some(true))
    statementAttributes.defaultTimestamp should be(Some(-76))
    statementAttributes.enableTrace should be(Some(true))
    statementAttributes.serialCl should be(Some(THREE))
    statementAttributes.fetchSize should be(Some(3))
    statementAttributes.retryPolicy should be(Some(FallthroughRetryPolicy.INSTANCE))
    statementAttributes.pagingState should be(Some(pagingState))
    statementAttributes.cqlStatements should contain only "FOO"
  }

  it should "build statements from a SimpleStatement" in {
    val statementAttributes: DseCqlAttributes = cql("the-tag")
      .executeStatement(new SimpleStatement("Some CQL"))
      .build()
      .dseAttributes
    val statement: SimpleStatement = statementAttributes.statement
      .buildFromFeeders(Session("the-tag", 42))
      .get.asInstanceOf[SimpleStatement]
    statementAttributes.cqlStatements should contain only "Some CQL"
    statement.getQueryString() should be("Some CQL")
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
