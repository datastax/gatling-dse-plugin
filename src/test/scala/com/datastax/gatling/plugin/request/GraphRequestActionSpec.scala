package com.datastax.gatling.plugin.request

import java.util.concurrent.{Executor, TimeUnit}

import akka.actor.{ActorSystem, Props}
import akka.testkit.TestKitBase
import com.datastax.driver.core._
import com.datastax.driver.dse.DseSession
import com.datastax.driver.dse.graph.{GraphResultSet, RegularGraphStatement, SimpleGraphStatement}
import com.datastax.gatling.plugin.base.BaseSpec
import com.datastax.gatling.plugin.metrics.NoopMetricsLogger
import com.datastax.gatling.plugin.utils.GatlingTimingSource
import com.datastax.gatling.plugin.DseProtocol
import com.datastax.gatling.plugin.model.{DseGraphStatement, DseGraphAttributes}
import com.google.common.util.concurrent.{Futures, ListenableFuture}
import io.gatling.commons.validation.SuccessWrapper
import io.gatling.core.action.Exit
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.session.Session
import io.gatling.core.stats.StatsEngine
import org.easymock.EasyMock
import org.easymock.EasyMock._

class GraphRequestActionSpec extends BaseSpec with TestKitBase {
  implicit lazy val system = ActorSystem()
  val gatlingTestConfig = GatlingConfiguration.loadForTest()
  val dseSession = mock[DseSession]
  val dseGraphStatement = mock[DseGraphStatement]
  val pagingState = mock[PagingState]
  val statsEngine: StatsEngine = mock[StatsEngine]
  val gatlingSession = Session("scenario", 1)

  def getTarget(dseAttributes: DseGraphAttributes): GraphRequestAction = {
    new GraphRequestAction(
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
    reset(dseGraphStatement, dseSession, pagingState, statsEngine)
  }

  override protected def afterAll(): Unit = {
    shutdown(system)
  }
  
  describe("Graph") {
    val statementCapture = EasyMock.newCapture[RegularGraphStatement]
    it("should enable all the Graph Attributes in DseAttributes") {
      val graphAttributes = DseGraphAttributes("test", dseGraphStatement,
        cl = Some(ConsistencyLevel.ANY),
        userOrRole = Some("test_user"),
        readTimeout = Some(12),
        defaultTimestamp = Some(1498167845000L),
        idempotent = Some(true),
        readCL = Some(ConsistencyLevel.LOCAL_QUORUM),
        writeCL = Some(ConsistencyLevel.LOCAL_QUORUM),
        graphName = Some("MyGraph"),
        graphLanguage = Some("english"),
        graphSource = Some("mysource"),
        graphInternalOptions = Some(Seq(("get", "this"))),
        graphTransformResults = None
      )

      expecting {
        dseGraphStatement.buildFromSession(gatlingSession).andReturn(new SimpleGraphStatement("g.V()").success)
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
      val graphAttributes = DseGraphAttributes(
        "test",
        dseGraphStatement,
        graphName = Some("MyGraph"),
        isSystemQuery = Some(true),
      )

      expecting {
        dseGraphStatement.buildFromSession(gatlingSession).andReturn(new SimpleGraphStatement("g.V()").success)
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
