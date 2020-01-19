package com.datastax.gatling.plugin

import java.nio.ByteBuffer

import com.datastax.gatling.plugin.base.BaseSpec
import com.datastax.gatling.plugin.exceptions.DseCqlStatementException
import com.datastax.gatling.plugin.model._
import com.datastax.gatling.plugin.utils.CqlPreparedStatementUtil
import com.datastax.oss.driver.api.core.`type`.{DataType, DataTypes}
import com.datastax.oss.driver.api.core.cql._
import io.gatling.commons.validation._
import io.gatling.core.session.Session
import io.gatling.core.session.el.ElCompiler
import org.easymock.EasyMock._

import scala.collection.JavaConverters._

class DseCqlStatementSpec extends BaseSpec {

  val prepared = mock[PreparedStatement]
  val mockColDefinitions = mock[ColumnDefinitions]
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

      val stmt = SimpleStatement.builder("select * from keyspace.table where id = 5").build()
      val result = DseCqlSimpleStatement(stmt).buildFromSession(validGatlingSession)

      result shouldBe a[Success[_]]
      result.get.build.getQuery shouldBe stmt.getQuery
    }
  }

  describe("DseCqlBoundStatementWithPassedParams") {

    val e1 = ElCompiler.compile[AnyRef]("${foo}")
    val e2 = ElCompiler.compile[AnyRef]("${bar}")

    val mockBuilder = mock[BoundStatementBuilder]

    it("correctly bind values to a prepared statement") {

      expecting {
        prepared.bind(fooValue, barValue).andReturn(mockBoundStatement)
        mockBuilder.build().andReturn(mockBoundStatement)
      }

      whenExecuting(prepared, mockCqlTypes, mockBoundStatement) {
        DseCqlBoundStatementWithPassedParams(mockCqlTypes, prepared, (_) => mockBuilder, e1, e2)
          .buildFromSession(validGatlingSession) shouldBe a[Success[_]]
      }
    }

    it("should fail if the expression is wrong and return the 1st error") {

      expecting {
        prepared.getVariableDefinitions.andStubReturn(mockColDefinitions)
      }

      whenExecuting(prepared, mockCqlTypes, mockBoundStatement) {
        val r = DseCqlBoundStatementWithPassedParams(mockCqlTypes, prepared, e1, e2)
          .buildFromSession(invalidGatlingSession)
        r shouldBe a[Failure]
        r shouldBe "No attribute named 'foo' is defined".failure
      }
    }
  }

  describe("DseCqlBoundStatementWithParamList") {

    val validParamList = Seq("foo", "bar")
    val paramsList = List(DataTypes.TEXT, DataTypes.INT)

    val mockBuilder = mock[BoundStatementBuilder]

    it("correctly bind values to a prepared statement") {

      expecting {
        prepared.bind().andReturn(mockBoundStatement)
        mockCqlTypes.getParamsList(prepared).andReturn(paramsList)
        mockCqlTypes.bindParamByOrder(validGatlingSession, mockBuilder, DataTypes.TEXT, "foo", 0)
            .andReturn(mockBuilder)
        mockCqlTypes.bindParamByOrder(validGatlingSession, mockBuilder, DataTypes.INT, "bar", 1)
            .andReturn(mockBuilder)
        mockBuilder.build().andReturn(mockBoundStatement)
      }

      whenExecuting(prepared, mockCqlTypes, mockBoundStatement) {
        DseCqlBoundStatementWithParamList(mockCqlTypes, prepared, validParamList, (_) => mockBuilder)
          .buildFromSession(validGatlingSession) shouldBe a[Success[_]]
      }
    }
  }

  describe("DseCqlBoundStatementNamed") {

    val mockBuilder = mock[BoundStatementBuilder]

    it("correctly bind values to a prepared statement") {

      expecting {
        prepared.bind().andReturn(mockBoundStatement)
        mockCqlTypes.getParamsMap(prepared).andReturn(Map(fooKey -> DataTypes.INT))
        mockCqlTypes.bindParamByName(validGatlingSession, mockBuilder, DataTypes.INT, "foo")
            .andReturn(mockBuilder)
        mockBuilder.build().andReturn(mockBoundStatement)
      }

      whenExecuting(prepared, mockCqlTypes, mockBoundStatement) {
        DseCqlBoundStatementNamed(mockCqlTypes, prepared, (_) => mockBuilder)
          .buildFromSession(validGatlingSession) shouldBe a[Success[_]]
      }
    }
  }

  describe("DseCqlBoundStatementNamedFromSession") {

    val mockBuilder = mock[BoundStatementBuilder]

    it("correctly bind values to a prepared statement in session") {
      val sessionWithStatement: Session = validGatlingSession.set("statementKey", prepared)
      expecting {
        prepared.bind().andReturn(mockBoundStatement)
        mockCqlTypes.getParamsMap(prepared).andReturn(Map(fooKey -> DataTypes.INT))
        mockCqlTypes.bindParamByName(sessionWithStatement, mockBuilder, DataTypes.INT, fooKey)
          .andReturn(mockBuilder)
        mockBuilder.build().andReturn(mockBoundStatement)
      }

      whenExecuting(prepared, mockCqlTypes, mockBoundStatement) {
        DseCqlBoundStatementNamedFromSession(mockCqlTypes, "statementKey", (_) => mockBuilder)
          .buildFromSession(sessionWithStatement) shouldBe a[Success[_]]
      }
    }

    it("should fail if the prepared statement is not in session") {
      expecting {
      }
      whenExecuting(prepared, mockCqlTypes, mockBoundStatement) {
        val thrown = intercept[DseCqlStatementException] {
          DseCqlBoundStatementNamedFromSession(mockCqlTypes, "statementKey")
            .buildFromSession(validGatlingSession) shouldBe a[Failure]
        }
        thrown.getMessage shouldBe "Passed sessionKey: {statementKey} does not exist in Session."
      }
    }
  }

  describe("DseCqlBoundBatchStatement") {

    val mockBuilder = mock[BoundStatementBuilder]

    it("correctly bind values to a prepared statement") {

      expecting {
        prepared.bind().andReturn(mockBoundStatement)
        mockCqlTypes.getParamsMap(prepared).andReturn(Map(fooKey -> DataTypes.INT))
        mockCqlTypes.bindParamByName(validGatlingSession, mockBuilder, DataTypes.INT, fooKey)
            .andReturn(mockBuilder).anyTimes()
        mockBuilder.build().andReturn(mockBoundStatement)
      }

      whenExecuting(prepared, mockCqlTypes, mockBoundStatement, mockBuilder) {
        DseCqlBoundBatchStatement(mockCqlTypes, Seq(prepared), (_) => mockBuilder)
          .buildFromSession(validGatlingSession) shouldBe a[Success[_]]
      }
    }
  }

  describe("DseCqlCustomPayloadStatement") {

    val stmt = SimpleStatement.builder("select * from keyspace.table where id = 5").build()

    it("should succeed with a passed SimpleStatement", CqlTest) {

      val expectedCustomPayload = Map("test" -> ByteBuffer.wrap(Array(12.toByte)))
      val payloadGatlingSession = new Session("name", 1, Map(
        "payload" -> expectedCustomPayload)
      )

      val result = DseCqlCustomPayloadStatement(stmt, "payload")
        .buildFromSession(payloadGatlingSession)

      result shouldBe a[Success[_]]
      val resultStmt = result.get.build
      resultStmt.getCustomPayload shouldBe expectedCustomPayload.asJava
      resultStmt.getQuery shouldBe stmt.getQuery
    }

    it("should fail with non existent sessionKey", CqlTest) {

      val thrown = intercept[DseCqlStatementException] {
        DseCqlCustomPayloadStatement(stmt, "payload")
          .buildFromSession(validGatlingSession) shouldBe a[Failure]
      }
      thrown.getMessage shouldBe  s"Passed sessionKey: {payload} does not exist in Session."
    }


    it("should fail with an invalid payload", CqlTest) {
      val payloadGatlingSession = new Session("name", 1, Map("payload" -> 12))
      DseCqlCustomPayloadStatement(stmt, "payload")
        .buildFromSession(payloadGatlingSession) shouldBe a[Failure]
    }
  }
}

