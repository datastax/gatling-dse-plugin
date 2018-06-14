package com.datastax.gatling.plugin

import java.nio.ByteBuffer

import com.datastax.driver.core.ColumnDefinitions.Definition
import com.datastax.driver.core._
import com.datastax.gatling.plugin.base.BaseSpec
import com.datastax.gatling.plugin.exceptions.DseCqlStatementException
import com.datastax.gatling.plugin.model._
import com.datastax.gatling.plugin.utils.CqlPreparedStatementUtil
import io.gatling.commons.validation._
import io.gatling.core.session.Session
import io.gatling.core.session.el.ElCompiler
import org.easymock.EasyMock._

import scala.collection.JavaConverters._


class DseCqlStatementSpec extends BaseSpec {

  val prepared = mock[PreparedStatement]
  val mockColDefinitions = mock[ColumnDefinitions]
  val mockDefinitions = mock[Definition]
  val mockDefinitionId = mock[Definition]
  val mockBoundStatement = mock[BoundStatement]
  val mockCqlTypes = mock[CqlPreparedStatementUtil]

  val fooKey = "foo"
  val fooValue = Integer.valueOf(5)
  val barKey = "bar"
  val barValue = "barValue"

  val validGatlingSession = new Session("name", 1, Map(fooKey -> fooValue, barKey -> barValue))
  val invalidGatlingSession = new Session("name", 1, Map("fu" -> Integer.valueOf(5), "buz" -> "BaZ"))


  val invalidStmt = "select * from test where invalid = 'test'"
  val invalidExceptionError = "Prepared Statements must have at least one settable param. Query: " + invalidStmt

  before {
    reset(prepared, mockBoundStatement, mockCqlTypes)
  }


  describe("DseCqlSimpleStatement") {

    it("should succeed with a passed SimpleStatement", CqlTest) {

      val stmt = new SimpleStatement("select * from keyspace.table where id = 5")
      val result = DseCqlSimpleStatement(stmt).buildFromFeeders(validGatlingSession)

      result shouldBe a[Success[_]]
      result.get.toString shouldBe stmt.toString
    }

  }


  describe("DseCqlBoundStatementWithPassedParams") {

    val e1 = ElCompiler.compile[AnyRef]("${foo}")
    val e2 = ElCompiler.compile[AnyRef]("${bar}")

    it("correctly bind values to a prepared statement") {

      expecting {
        prepared.bind(fooValue, barValue).andReturn(mockBoundStatement)
        mockCqlTypes.checkIsValidPreparedStatement(prepared).andReturn(true)
      }

      whenExecuting(prepared, mockCqlTypes, mockBoundStatement) {
        DseCqlBoundStatementWithPassedParams(mockCqlTypes, prepared, e1, e2)
          .buildFromFeeders(validGatlingSession) shouldBe a[Success[_]]
      }
    }


    it("should fail if the expression is wrong and return the 1st error") {

      expecting {
        prepared.getVariables.andStubReturn(mockColDefinitions)
        mockCqlTypes.checkIsValidPreparedStatement(prepared).andReturn(true)
      }

      whenExecuting(prepared, mockCqlTypes, mockBoundStatement) {
        val r = DseCqlBoundStatementWithPassedParams(mockCqlTypes, prepared, e1, e2)
          .buildFromFeeders(invalidGatlingSession)
        r shouldBe a[Failure]
        r shouldBe "No attribute named 'foo' is defined".failure
      }
    }


    it("should fail if the prepared statement is invalid") {

      expecting {
        prepared.getQueryString.andReturn(invalidStmt)
        mockCqlTypes.checkIsValidPreparedStatement(prepared).andReturn(false)
      }

      whenExecuting(prepared, mockCqlTypes, mockBoundStatement) {
        val thrown = intercept[DseCqlStatementException] {
          DseCqlBoundStatementWithPassedParams(mockCqlTypes, prepared, e1, e2)
            .buildFromFeeders(validGatlingSession) shouldBe a[Success[_]]
        }
        thrown.getMessage shouldBe invalidExceptionError
      }

    }

  }


  describe("DseCqlBoundStatementWithParamList") {

    val validParamList = Seq("foo", "bar")
    val paramsList = List[DataType.Name](DataType.Name.TEXT, DataType.Name.INT)

    it("correctly bind values to a prepared statement") {

      expecting {
        prepared.bind().andReturn(mockBoundStatement)
        mockCqlTypes.checkIsValidPreparedStatement(prepared).andReturn(true)
        mockCqlTypes.getParamsList(prepared).andReturn(paramsList)
        mockCqlTypes.bindParamByOrder(validGatlingSession, mockBoundStatement, DataType.Name.TEXT, "foo", 0)
            .andReturn(mockBoundStatement)
        mockCqlTypes.bindParamByOrder(validGatlingSession, mockBoundStatement, DataType.Name.INT, "bar", 1)
            .andReturn(mockBoundStatement)
      }

      whenExecuting(prepared, mockCqlTypes, mockBoundStatement) {
        DseCqlBoundStatementWithParamList(mockCqlTypes, prepared, validParamList)
          .buildFromFeeders(validGatlingSession) shouldBe a[Success[_]]
      }
    }

    it("should fail if the session is empty of params") {

      expecting {
        mockCqlTypes.checkIsValidPreparedStatement(prepared).andReturn(true)
      }

      whenExecuting(prepared, mockColDefinitions, mockDefinitions, mockDefinitionId, mockCqlTypes) {
        val thrown = intercept[DseCqlStatementException] {
          DseCqlBoundStatementWithParamList(mockCqlTypes, prepared, List())
            .buildFromFeeders(validGatlingSession) shouldBe a[Success[_]]
        }
        thrown.getMessage shouldBe "Gatling session key list cannot be empty"
      }

    }

    it("should fail if the prepared statement is invalid") {

      expecting {
        prepared.getQueryString.andReturn(invalidStmt)
        mockCqlTypes.checkIsValidPreparedStatement(prepared).andReturn(false)
      }

      whenExecuting(prepared, mockCqlTypes, mockBoundStatement) {
        val thrown = intercept[DseCqlStatementException] {
          DseCqlBoundStatementWithParamList(mockCqlTypes, prepared, List())
            .buildFromFeeders(validGatlingSession) shouldBe a[Success[_]]
        }
        thrown.getMessage shouldBe invalidExceptionError
      }

    }

  }



  describe("DseCqlBoundStatementNamed") {

    it("correctly bind values to a prepared statement") {

      expecting {
        prepared.bind().andReturn(mockBoundStatement)
        mockCqlTypes.checkIsValidPreparedStatement(prepared).andReturn(true)
        mockCqlTypes.getParamsMap(prepared).andReturn(Map(fooKey -> DataType.Name.INT))
        mockCqlTypes.bindParamByName(validGatlingSession, mockBoundStatement, DataType.Name.INT, "foo")
            .andReturn(mockBoundStatement)
      }

      whenExecuting(prepared, mockCqlTypes, mockBoundStatement) {
        DseCqlBoundStatementNamed(mockCqlTypes, prepared)
          .buildFromFeeders(validGatlingSession) shouldBe a[Success[_]]
      }
    }


    it("should fail if the prepared statement is invalid") {

      expecting {
        prepared.getQueryString.andReturn(invalidStmt)
        mockCqlTypes.checkIsValidPreparedStatement(prepared).andReturn(false)
      }

      whenExecuting(prepared, mockCqlTypes, mockBoundStatement) {
        val thrown = intercept[DseCqlStatementException] {
          DseCqlBoundStatementNamed(mockCqlTypes, prepared)
            .buildFromFeeders(validGatlingSession) shouldBe a[Failure]
        }
        thrown.getMessage shouldBe invalidExceptionError
      }

    }

  }




  describe("DseCqlBoundStatementNamedFromSession") {
    it("correctly bind values to a prepared statement in session") {
      val sessionWithStatement: Session = validGatlingSession.set("statementKey", prepared)
      expecting {
        prepared.bind().andReturn(mockBoundStatement)
        mockCqlTypes.checkIsValidPreparedStatement(prepared).andReturn(true)
        mockCqlTypes.getParamsMap(prepared).andReturn(Map(fooKey -> DataType.Name.INT))
        mockCqlTypes.bindParamByName(sessionWithStatement, mockBoundStatement, DataType.Name.INT, "foo")
          .andReturn(mockBoundStatement)
      }
      whenExecuting(prepared, mockCqlTypes, mockBoundStatement) {
        DseCqlBoundStatementNamedFromSession(mockCqlTypes, "statementKey")
          .buildFromFeeders(sessionWithStatement) shouldBe a[Success[_]]
      }
    }

    it("should fail if the prepared statement is not in session") {
      expecting {
      }
      whenExecuting(prepared, mockCqlTypes, mockBoundStatement) {
        val thrown = intercept[DseCqlStatementException] {
          DseCqlBoundStatementNamedFromSession(mockCqlTypes, "statementKey")
            .buildFromFeeders(validGatlingSession) shouldBe a[Failure]
        }
        thrown.getMessage shouldBe "Passed sessionKey: {statementKey} does not exist in Session."
      }
    }
  }




  describe("DseCqlBoundBatchStatement") {

    it("correctly bind values to a prepared statement") {

      expecting {
        mockBoundStatement.getOutgoingPayload.andReturn(Map("test" -> ByteBuffer.wrap(Array(12.toByte))).asJava)
        mockBoundStatement.getOutgoingPayload.andReturn(Map("test" -> ByteBuffer.wrap(Array(12.toByte))).asJava)
        prepared.bind().andReturn(mockBoundStatement)
        mockCqlTypes.checkIsValidPreparedStatement(prepared).andReturn(true)
        mockCqlTypes.getParamsMap(prepared).andReturn(Map(fooKey -> DataType.Name.INT))
        mockCqlTypes.bindParamByName(validGatlingSession, mockBoundStatement, DataType.Name.INT, "foo")
            .andReturn(mockBoundStatement)
      }

      whenExecuting(prepared, mockCqlTypes, mockBoundStatement) {
        DseCqlBoundBatchStatement(mockCqlTypes, Seq(prepared))
          .buildFromFeeders(validGatlingSession) shouldBe a[Success[_]]
      }
    }


    it("should fail if the prepared statement is invalid") {

      expecting {
        prepared.getQueryString.andReturn(invalidStmt)
        mockCqlTypes.checkIsValidPreparedStatement(prepared).andReturn(false)
      }

      whenExecuting(prepared, mockCqlTypes, mockBoundStatement) {
        val thrown = intercept[DseCqlStatementException] {
          DseCqlBoundBatchStatement(mockCqlTypes, Seq(prepared))
            .buildFromFeeders(validGatlingSession) shouldBe a[Failure]
        }
        thrown.getMessage shouldBe invalidExceptionError
      }

    }

  }


  describe("DseCqlCustomPayloadStatement") {

    val stmt = new SimpleStatement("select * from keyspace.table where id = 5")

    it("should succeed with a passed SimpleStatement", CqlTest) {

      val payloadGatlingSession = new Session("name", 1, Map(
        "payload" -> Map("test" -> ByteBuffer.wrap(Array(12.toByte))))
      )

      val result = DseCqlCustomPayloadStatement(stmt, "payload")
        .buildFromFeeders(payloadGatlingSession)

      result shouldBe a[Success[_]]
      result.get.toString shouldBe stmt.toString
    }

    it("should fail with non existent sessionKey", CqlTest) {

      val thrown = intercept[DseCqlStatementException] {
        DseCqlCustomPayloadStatement(stmt, "payload")
          .buildFromFeeders(validGatlingSession) shouldBe a[Failure]
      }
      thrown.getMessage shouldBe  s"Passed sessionKey: {payload} does not exist in Session."
    }


    it("should fail with an invalid payload", CqlTest) {
      val payloadGatlingSession = new Session("name", 1, Map("payload" -> 12))
      DseCqlCustomPayloadStatement(stmt, "payload")
        .buildFromFeeders(payloadGatlingSession) shouldBe a[Failure]
    }

  }

}

