package com.datastax.gatling.plugin.request

import java.nio.ByteBuffer
import java.time.Duration
import java.util.concurrent.{CompletableFuture, CompletionStage}

import akka.actor.{ActorSystem, Props}
import akka.testkit.TestKitBase
import ch.qos.logback.classic.{Level, Logger}
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.datastax.gatling.plugin.base.BaseSpec
import com.datastax.gatling.plugin.metrics.NoopMetricsLogger
import com.datastax.gatling.plugin.utils.GatlingTimingSource
import com.datastax.gatling.plugin.DseProtocol
import com.datastax.gatling.plugin.model.{DseCqlAttributes, DseCqlStatement}
import com.datastax.oss.driver.api.core.{ConsistencyLevel, CqlSession}
import com.datastax.oss.driver.api.core.cql.{AsyncResultSet, SimpleStatement, SimpleStatementBuilder}
import com.datastax.oss.driver.api.core.metadata.Node
import com.datastax.oss.driver.api.core.metadata.token.Token
import io.gatling.commons.validation.SuccessWrapper
import io.gatling.core.action.Exit
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.session.Session
import io.gatling.core.stats.StatsEngine
import org.easymock.EasyMock
import org.easymock.EasyMock._
import org.slf4j.LoggerFactory

class CqlRequestActionSpec extends BaseSpec with TestKitBase {
  implicit lazy val system:ActorSystem = ActorSystem()
  val gatlingTestConfig: GatlingConfiguration = GatlingConfiguration.loadForTest()
  val cqlSession: CqlSession = mock[CqlSession]
  val dseCqlStatement: DseCqlStatement[SimpleStatement,SimpleStatementBuilder] = mock[DseCqlStatement[SimpleStatement,SimpleStatementBuilder]]
  val node:Node = mock[Node]
  val pageSize = 3
  val pagingState: ByteBuffer = mock[ByteBuffer]
  val queryTimestamp = 123L
  val routingKey:ByteBuffer = mock[ByteBuffer]
  val routingKeyspace = "some_keyspace"
  val routingToken:Token = mock[Token]
  val timeout:Duration = Duration.ofHours(1)
  val statsEngine: StatsEngine = mock[StatsEngine]
  val gatlingSession = Session("scenario", 1)

  def getTarget(dseAttributes: DseCqlAttributes[SimpleStatement,SimpleStatementBuilder]): CqlRequestAction[SimpleStatement,SimpleStatementBuilder] = {
    new CqlRequestAction(
      "sample-dse-request",
      new Exit(system.actorOf(Props[DseRequestActor]), statsEngine),
      system,
      statsEngine,
      DseProtocol(cqlSession),
      dseAttributes,
      NoopMetricsLogger(),
      executorServiceForTests(),
      GatlingTimingSource())
  }

  private def mockAsyncResultSetFuture(): CompletionStage[AsyncResultSet] = CompletableFuture.completedFuture(mock[AsyncResultSet])

  before {
    reset(dseCqlStatement, cqlSession, pagingState, statsEngine)
  }

  override protected def afterAll(): Unit = {
    shutdown(system)
  }

  describe("CQL") {
    val statementCapture = EasyMock.newCapture[SimpleStatement]
    it("should have default CQL attributes set if nothing passed") {
      val cqlAttributesWithDefaults = DseCqlAttributes(
        "test",
        dseCqlStatement)

      expecting {
        dseCqlStatement.buildFromSession(gatlingSession) andReturn(SimpleStatement.builder("select * from test")
          .success)
        cqlSession.executeAsync(capture(statementCapture)) andReturn mockAsyncResultSetFuture()
      }

      whenExecuting(dseCqlStatement, cqlSession) {
        getTarget(cqlAttributesWithDefaults).sendQuery(gatlingSession)
      }

      val capturedStatement = statementCapture.getValue
      capturedStatement shouldBe a[SimpleStatement]
      capturedStatement.getConsistencyLevel shouldBe null
      capturedStatement.getSerialConsistencyLevel shouldBe null
      capturedStatement.getPageSize should be <= 0
      capturedStatement.isIdempotent shouldBe null
      capturedStatement.isTracing shouldBe false
      capturedStatement.getQuery should be("select * from test")
    }

    it("should enable all the CQL Attributes in DseAttributes") {
      val cqlAttributes = new DseCqlAttributes[SimpleStatement,SimpleStatementBuilder](
        "test",
        dseCqlStatement,
        cl = Some(ConsistencyLevel.ANY),
        idempotent = Some(true),
        node = Some(node),
        enableTrace = Some(true),
        pagingState = Some(pagingState),
        pageSize = Some(pageSize),
        queryTimestamp = Some(queryTimestamp),
        routingKey = Some(routingKey),
        routingKeyspace = Some(routingKeyspace),
        routingToken = Some(routingToken),
        serialCl = Some(ConsistencyLevel.LOCAL_SERIAL),
        timeout = Some(timeout))

      expecting {
        dseCqlStatement.buildFromSession(gatlingSession) andReturn(SimpleStatement.builder("select * from test")
          .success)
        cqlSession.executeAsync(capture(statementCapture)) andReturn mockAsyncResultSetFuture()
      }

      whenExecuting(dseCqlStatement, cqlSession) {
        getTarget(cqlAttributes).sendQuery(gatlingSession)
      }

      val capturedStatement = statementCapture.getValue
      capturedStatement shouldBe a[SimpleStatement]
      capturedStatement.getConsistencyLevel shouldBe ConsistencyLevel.ANY
      capturedStatement.isIdempotent shouldBe true
      capturedStatement.getNode shouldBe node
      capturedStatement.isTracing shouldBe true
      capturedStatement.getPageSize shouldBe pageSize
      capturedStatement.getPagingState shouldBe pagingState
      capturedStatement.getQueryTimestamp shouldBe queryTimestamp
      capturedStatement.getRoutingKey shouldBe routingKey
      capturedStatement.getRoutingKeyspace.toString shouldBe routingKeyspace
      capturedStatement.getRoutingToken shouldBe routingToken
      capturedStatement.getSerialConsistencyLevel shouldBe ConsistencyLevel.LOCAL_SERIAL
      capturedStatement.getTimeout shouldBe timeout
    }

    it("should log exceptions encountered") {
      val errorMessage = "Invalid format: \"2016-11-16 06:43:19.77\" is malformed at \" 06:43:19.77\""

      expecting {
        dseCqlStatement.buildFromSession(gatlingSession).andThrow(new IllegalArgumentException(errorMessage))
      }

      val cqlAttributesWithDefaults = DseCqlAttributes(
        "test",
        dseCqlStatement)

      val cqlRequestAction = getTarget(cqlAttributesWithDefaults)

      val classLogger = LoggerFactory.getLogger(classOf[CqlRequestAction[SimpleStatement,SimpleStatementBuilder]]).asInstanceOf[Logger]
      val listAppender: ListAppender[ILoggingEvent] = new ListAppender[ILoggingEvent]
      listAppender.start()
      classLogger.addAppender(listAppender)

      whenExecuting(dseCqlStatement, cqlSession) {
        cqlRequestAction.sendQuery(gatlingSession)
      }

      assert(listAppender.list.size() > 0)

      val logEntry = listAppender.list.get(0)

      assert(logEntry.getLevel == Level.ERROR)
      assert(logEntry.getFormattedMessage.contains(errorMessage))
    }
  }
}
