package com.datastax.gatling.plugin.request

import java.nio.ByteBuffer
import java.util.concurrent.{Executor, TimeUnit}

import akka.actor.{ActorSystem, Props}
import akka.testkit.TestKitBase
import ch.qos.logback.classic.{Level, Logger}
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.datastax.dse.driver.api.core.DseSession
import com.datastax.gatling.plugin.base.BaseSpec
import com.datastax.gatling.plugin.metrics.NoopMetricsLogger
import com.datastax.gatling.plugin.utils.GatlingTimingSource
import com.datastax.gatling.plugin.DseProtocol
import com.datastax.gatling.plugin.model.{DseCqlAttributes, DseCqlStatement}
import com.datastax.oss.driver.api.core.cql.{ResultSet, SimpleStatement, Statement}
import com.datastax.oss.protocol.internal.ProtocolConstants.ConsistencyLevel
import com.google.common.util.concurrent.{Futures, ListenableFuture}
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
  val dseSession: DseSession = mock[DseSession]
  val dseCqlStatement: DseCqlStatement[SimpleStatement] = mock[DseCqlStatement[SimpleStatement]]
  val pagingState: ByteBuffer = mock[ByteBuffer]
  val statsEngine: StatsEngine = mock[StatsEngine]
  val gatlingSession = Session("scenario", 1)

  def getTarget(dseAttributes: DseCqlAttributes[SimpleStatement]): CqlRequestAction = {
    new CqlRequestAction(
      "sample-dse-request",
      new Exit(system.actorOf(Props[DseRequestActor]), statsEngine),
      system,
      statsEngine,
      DseProtocol(dseSession),
      dseAttributes,
      NoopMetricsLogger(),
      executorServiceForTests(),
      GatlingTimingSource())
  }

  private def mockResultSetFuture(): ResultSetFuture = new ResultSetFuture {
    val delegate: ListenableFuture[ResultSet] = Futures.immediateFuture(mock[ResultSet])
    override def cancel(b: Boolean): Boolean = false
    override def getUninterruptibly: ResultSet = delegate.get()
    override def getUninterruptibly(duration: Long, timeUnit: TimeUnit): ResultSet = delegate.get(duration, timeUnit)
    override def addListener(listener: Runnable, executor: Executor): Unit = delegate.addListener(listener, executor)
    override def isCancelled: Boolean = delegate.isCancelled
    override def isDone: Boolean = delegate.isDone
    override def get(): ResultSet = delegate.get()
    override def get(timeout: Long, unit: TimeUnit): ResultSet = delegate.get(timeout, unit)
  }

  before {
    reset(dseCqlStatement, dseSession, pagingState, statsEngine)
  }

  override protected def afterAll(): Unit = {
    shutdown(system)
  }

  describe("CQL") {
    val statementCapture = EasyMock.newCapture[Statement]
    it("should have default CQL attributes set if nothing passed") {
      val cqlAttributesWithDefaults = DseCqlAttributes(
        "test",
        dseCqlStatement)

      expecting {
        dseCqlStatement.buildFromSession(gatlingSession).andReturn(new SimpleStatement("select * from test")
          .success)
        dseSession.executeAsync(capture(statementCapture)) andReturn mockResultSetFuture()
      }

      whenExecuting(dseCqlStatement, dseSession) {
        getTarget(cqlAttributesWithDefaults).sendQuery(gatlingSession)
      }

      val capturedStatement = statementCapture.getValue
      capturedStatement shouldBe a[SimpleStatement]
      capturedStatement.getConsistencyLevel shouldBe null
      capturedStatement.getSerialConsistencyLevel shouldBe null
      capturedStatement.getFetchSize shouldBe 0
      capturedStatement.getDefaultTimestamp shouldBe -9223372036854775808L
      capturedStatement.getReadTimeoutMillis shouldBe -2147483648
      capturedStatement.getRetryPolicy shouldBe null
      capturedStatement.isIdempotent shouldBe null
      capturedStatement.isTracing shouldBe false
      capturedStatement.getQueryString should be("select * from test")
    }

    it("should enable all the CQL Attributes in DseAttributes") {
      val cqlAttributes = DseCqlAttributes(
        "test",
        dseCqlStatement,
        cl = Some(ConsistencyLevel.ANY),
        userOrRole = Some("test_user"),
        readTimeout = Some(12),
        defaultTimestamp = Some(1498167845000L),
        idempotent = Some(true),
        fetchSize = Some(50),
        serialCl = Some(ConsistencyLevel.LOCAL_SERIAL),
        retryPolicy = Some(FallthroughRetryPolicy.INSTANCE),
        enableTrace = Some(true))

      expecting {
        dseCqlStatement.buildFromSession(gatlingSession).andReturn(new SimpleStatement("select * from test")
          .success)
        dseSession.executeAsync(capture(statementCapture)) andReturn mockResultSetFuture()
      }

      whenExecuting(dseCqlStatement, dseSession) {
        getTarget(cqlAttributes).sendQuery(gatlingSession)
      }

      val capturedStatement = statementCapture.getValue
      capturedStatement shouldBe a[SimpleStatement]
      capturedStatement.getConsistencyLevel shouldBe ConsistencyLevel.ANY
      capturedStatement.getDefaultTimestamp shouldBe 1498167845000L
      capturedStatement.getReadTimeoutMillis shouldBe 12
      capturedStatement.isIdempotent shouldBe true
      capturedStatement.getFetchSize shouldBe 50
      capturedStatement.getSerialConsistencyLevel shouldBe ConsistencyLevel.LOCAL_SERIAL
      capturedStatement.getRetryPolicy shouldBe FallthroughRetryPolicy.INSTANCE
      capturedStatement.getQueryString should be("select * from test")
      capturedStatement.isTracing shouldBe true
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

      val classLogger = LoggerFactory.getLogger(classOf[CqlRequestAction]).asInstanceOf[Logger]
      val listAppender: ListAppender[ILoggingEvent] = new ListAppender[ILoggingEvent]
      listAppender.start()
      classLogger.addAppender(listAppender)

      whenExecuting(dseCqlStatement, dseSession) {
        cqlRequestAction.sendQuery(gatlingSession)
      }

      assert(listAppender.list.size() > 0)

      val logEntry = listAppender.list.get(0)

      assert(logEntry.getLevel == Level.ERROR)
      assert(logEntry.getFormattedMessage.contains(errorMessage))
    }
  }
}
