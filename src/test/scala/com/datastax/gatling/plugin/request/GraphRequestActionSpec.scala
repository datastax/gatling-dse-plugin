package com.datastax.gatling.plugin.request

import java.nio.ByteBuffer
import java.time.Duration
import java.util.concurrent.{CompletableFuture, CompletionStage}

import akka.actor.{ActorSystem, Props}
import akka.testkit.TestKitBase
import com.datastax.dse.driver.api.core.graph.{ScriptGraphStatement => ScriptS, ScriptGraphStatementBuilder => ScriptB, _}
import com.datastax.gatling.plugin.base.BaseSpec
import com.datastax.gatling.plugin.metrics.NoopMetricsLogger
import com.datastax.gatling.plugin.utils.GatlingTimingSource
import com.datastax.gatling.plugin.DseProtocol
import com.datastax.gatling.plugin.model.{DseGraphAttributes, DseGraphStatement}
import com.datastax.oss.driver.api.core.{ConsistencyLevel, CqlSession}
import com.datastax.oss.driver.api.core.metadata.Node
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
  val cqlSession = mock[CqlSession]
  val dseGraphStatement = mock[DseGraphStatement[ScriptS,ScriptB]]
  val node:Node = mock[Node]
  val readConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM
  val subProtocol = "graph-binary-3.0"
  val timeout:Duration = Duration.ofHours(1)
  val timestamp = 123L
  val traversalSource = "g.V()"
  val writeConsistencyLevel = ConsistencyLevel.LOCAL_QUORUM

  val pagingState:ByteBuffer = mock[ByteBuffer]
  val statsEngine: StatsEngine = mock[StatsEngine]
  val gatlingSession = Session("scenario", 1)

  def getTarget(dseAttributes: DseGraphAttributes[ScriptS,ScriptB]): GraphRequestAction[ScriptS,ScriptB] = {
    new GraphRequestAction(
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

  private def mockAsyncGraphResultSetFuture(): CompletionStage[AsyncGraphResultSet] =
    CompletableFuture.completedFuture(mock[AsyncGraphResultSet])

  before {
    reset(dseGraphStatement, cqlSession, pagingState, statsEngine)
  }

  override protected def afterAll(): Unit = {
    shutdown(system)
  }

  describe("Graph") {
    val statementCapture = EasyMock.newCapture[ScriptS]
    it("should enable all the Graph Attributes in DseAttributes") {
      val graphAttributes = new DseGraphAttributes("test", dseGraphStatement,
        cl = Some(ConsistencyLevel.ANY),
        idempotent = Some(true),
        node = Some(node),
        graphName = Some("MyGraph"),
        readCL = Some(readConsistencyLevel),
        subProtocol = Some(subProtocol),
        timeout = Some(timeout),
        timestamp = Some(timestamp),
        traversalSource = Some(traversalSource),
        writeCL = Some(writeConsistencyLevel)
      )

      expecting {
        dseGraphStatement.buildFromSession(gatlingSession) andReturn(ScriptS.builder("g.V()").success)
        cqlSession.executeAsync(capture(statementCapture)) andReturn mockAsyncGraphResultSetFuture()
      }

      whenExecuting(dseGraphStatement, cqlSession) {
        getTarget(graphAttributes).sendQuery(gatlingSession)
      }

      val capturedStatement = statementCapture.getValue
      capturedStatement shouldBe a[ScriptS]
      capturedStatement.getConsistencyLevel shouldBe ConsistencyLevel.ANY
      capturedStatement.isIdempotent shouldBe true
      capturedStatement.getNode shouldBe node
      capturedStatement.getGraphName shouldBe "MyGraph"
      capturedStatement.getReadConsistencyLevel shouldBe readConsistencyLevel
      capturedStatement.getSubProtocol shouldBe subProtocol
      capturedStatement.getTimeout shouldBe timeout
      capturedStatement.getTimestamp shouldBe timestamp
      capturedStatement.getTraversalSource shouldBe traversalSource
      capturedStatement.getWriteConsistencyLevel shouldBe writeConsistencyLevel
    }
  }
}
