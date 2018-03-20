package com.datastax.gatling.plugin.request

import java.util.concurrent.{Executor, TimeUnit}

import akka.actor.{ActorSystem, Props}
import akka.testkit.TestKitBase
import com.datastax.driver.core._
import com.datastax.driver.core.policies.FallthroughRetryPolicy
import com.datastax.driver.dse.DseSession
import com.datastax.driver.dse.graph.{GraphResultSet, RegularGraphStatement, SimpleGraphStatement}
import com.datastax.gatling.plugin.base.BaseSpec
import com.datastax.gatling.plugin.checks.DseCheck
import com.datastax.gatling.plugin.metrics.NoopMetricsLogger
import com.datastax.gatling.plugin.{DseCqlStatement, DseGraphStatement, DseProtocol}
import com.google.common.util.concurrent.{Futures, ListenableFuture}
import io.gatling.commons.validation.SuccessWrapper
import io.gatling.core.action.Exit
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.session.Session
import io.gatling.core.stats.StatsEngine
import org.easymock.EasyMock
import org.easymock.EasyMock._

class DseRequestActionSpec extends BaseSpec with TestKitBase {
  implicit lazy val system = ActorSystem()
  val gatlingTestConfig = GatlingConfiguration.loadForTest()
  val dseSession = mock[DseSession]
  val dseCqlStatement = mock[DseCqlStatement]
  val dseGraphStatement = mock[DseGraphStatement]
  val pagingState = mock[PagingState]
  val statsEngine: StatsEngine = mock[StatsEngine]
  val gatlingSession = Session("scenario", 1)

  def getTarget(dseAttributes: DseAttributes): DseRequestAction = {
    new DseRequestAction(
      "sample-dse-request",
      new Exit(system.actorOf(Props[DseRequestActor]), statsEngine),
      system,
      statsEngine,
      DseProtocol(dseSession),
      dseAttributes,
      NoopMetricsLogger(),
      system.actorOf(Props[DseRequestActor]))
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
    reset(dseCqlStatement, dseGraphStatement, dseSession, pagingState, statsEngine)
  }

  override protected def afterAll(): Unit = {
    shutdown(system)
  }

  describe("CQL") {
    val statementCapture = EasyMock.newCapture[RegularStatement]
    it("should have default CQL attributes set if nothing passed") {
      val cqlAttributesWithDefaults = DseAttributes(
        "test",
        dseCqlStatement)

      expecting {
        dseCqlStatement(gatlingSession).andReturn(new SimpleStatement("select * from test").success)
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

    it("should enabled all the CQL Attributes in DseAttributes") {
      val cqlAttributes = DseAttributes(
        "test",
        dseCqlStatement,
        cl = Some(ConsistencyLevel.ANY),
        checks = List.empty[DseCheck],
        userOrRole = Some("test_user"),
        readTimeout = Some(12),
        defaultTimestamp = Some(1498167845000L),
        idempotent = Some(true),
        cqlFetchSize = Some(50),
        cqlSerialCl = Some(ConsistencyLevel.LOCAL_SERIAL),
        cqlRetryPolicy = Some(FallthroughRetryPolicy.INSTANCE),
        cqlEnableTrace = Some(true))

      expecting {
        dseCqlStatement(gatlingSession).andReturn(new SimpleStatement("select * from test").success)
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
  }

  describe("Graph") {
    val statementCapture = EasyMock.newCapture[RegularGraphStatement]
    it("should enable all the Graph Attributes in DseAttributes") {
      val graphAttributes = DseAttributes("test", dseGraphStatement,
        cl = Some(ConsistencyLevel.ANY),
        checks = List.empty[DseCheck],
        userOrRole = Some("test_user"),
        readTimeout = Some(12),
        defaultTimestamp = Some(1498167845000L),
        idempotent = Some(true),
        graphReadCL = Some(ConsistencyLevel.LOCAL_QUORUM),
        graphWriteCL = Some(ConsistencyLevel.LOCAL_QUORUM),
        graphName = Some("MyGraph"),
        graphLanguage = Some("english"),
        graphSource = Some("mysource"),
        graphInternalOptions = Some(Seq(("get", "this"))),
        graphTransformResults = None
      )

      expecting {
        dseGraphStatement(gatlingSession).andReturn(new SimpleGraphStatement("g.V()").success)
        dseSession.executeGraphAsync(capture(statementCapture)) andReturn Futures.immediateFuture(mock[GraphResultSet])
      }

      whenExecuting(dseGraphStatement, dseSession) {
        getTarget(graphAttributes).sendQuery(gatlingSession)
      }

      val capturedStatement = statementCapture.getValue
      capturedStatement shouldBe a[SimpleGraphStatement]
      capturedStatement.getConsistencyLevel shouldBe ConsistencyLevel.ANY
      capturedStatement.getDefaultTimestamp shouldBe 1498167845000L
      capturedStatement.getReadTimeoutMillis shouldBe 12
      capturedStatement.isIdempotent shouldBe true
      capturedStatement.getGraphName shouldBe "MyGraph"
      capturedStatement.getGraphLanguage shouldBe "english"
      capturedStatement.getGraphReadConsistencyLevel shouldBe ConsistencyLevel.LOCAL_QUORUM
      capturedStatement.getGraphWriteConsistencyLevel shouldBe ConsistencyLevel.LOCAL_QUORUM
      capturedStatement.getGraphSource shouldBe "mysource"
      capturedStatement.isSystemQuery shouldBe false
      capturedStatement.getGraphInternalOption("get") shouldBe "this"
    }

    it("should override the graph name if system") {
      val graphAttributes = DseAttributes(
        "test",
        dseGraphStatement,
        graphName = Some("MyGraph"),
        graphSystemQuery = Some(true),
      )

      expecting {
        dseGraphStatement(gatlingSession).andReturn(new SimpleGraphStatement("g.V()").success)
        dseSession.executeGraphAsync(capture(statementCapture)) andReturn Futures.immediateFuture(mock[GraphResultSet])
      }

      whenExecuting(dseGraphStatement, dseSession) {
        getTarget(graphAttributes).sendQuery(gatlingSession)
      }

      val capturedStatement = statementCapture.getValue
      capturedStatement shouldBe a[SimpleGraphStatement]
      capturedStatement.getGraphName shouldBe null
      capturedStatement.isSystemQuery shouldBe true
    }
  }
}
