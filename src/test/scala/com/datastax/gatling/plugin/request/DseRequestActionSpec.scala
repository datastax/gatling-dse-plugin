package com.datastax.gatling.plugin.request

import java.util.concurrent.Executors

import akka.actor.{ActorSystem, Props}
import akka.testkit.TestKitBase
import com.datastax.driver.core._
import com.datastax.driver.core.exceptions.DriverException
import com.datastax.driver.core.policies.RetryPolicy
import com.datastax.driver.core.policies.RetryPolicy.RetryDecision
import com.datastax.driver.dse.DseSession
import com.datastax.driver.dse.graph.{GraphResultSet, RegularGraphStatement, SimpleGraphStatement}
import com.datastax.gatling.plugin.base.BaseSpec
import com.datastax.gatling.plugin.checks.DseCheck
import com.datastax.gatling.plugin.metrics.HistogramLogger
import com.datastax.gatling.plugin.{DseCqlStatement, DseGraphStatement, DseProtocol}
import com.google.common.util.concurrent.ListenableFuture
import io.gatling.commons.validation.SuccessWrapper
import io.gatling.core.CoreComponents
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.session.Session
import org.easymock.EasyMock
import org.easymock.EasyMock._

class DseRequestActionSpec extends BaseSpec with TestKitBase {
  implicit lazy val system = ActorSystem()
  val gatlingTestConfig = GatlingConfiguration.loadForTest()
  val mockDseSession = mock[DseSession]
  val mockDseCqlStatement = mock[DseCqlStatement]
  val mockDseGraphStatement = mock[DseGraphStatement]
  val mockActorSystem = mock[ActorSystem]
  val pagingState = mock[PagingState]
  val mockHistogramLogger = mock[HistogramLogger]
  val timeoutEnforcer = Executors.newSingleThreadScheduledExecutor()

  val gatlingSession = Session("scenario", 1)

  val queryUser = "test_user"
  val queryReadTimeout = 12
  val queryDefaultTimestamp = 1498167845000L
  val queryConsistencyLevel = ConsistencyLevel.ANY
  val queryIdempotent = true

  val coreComponents = CoreComponents(controller = null, throttler = null, statsEngine = null, exit = null, configuration = gatlingTestConfig)

  def getTarget(dseAttributes: DseAttributes): DseRequestAction = {
    new DseRequestAction("some-name", coreComponents.exit, mockActorSystem, coreComponents.statsEngine,
      DseProtocol(mockDseSession), dseAttributes, mockHistogramLogger, timeoutEnforcer, system.actorOf(Props[DseRequestActor])
    )
  }

  before {
    reset(mockDseCqlStatement, mockDseGraphStatement, mockDseSession, pagingState, mockHistogramLogger)
  }

  override protected def afterAll(): Unit = {
    timeoutEnforcer.shutdown()
    shutdown(system)
  }

  describe("CQL") {

    val fetchSize = 50
    val lwtCl = ConsistencyLevel.LOCAL_SERIAL
    val retryPolicy = new CustomRetryPolicy(2, 2, 3)

    val statementCapture = EasyMock.newCapture[RegularStatement]
    val cqlAttributes = DseAttributes("test", mockDseCqlStatement)

    it("should have default CQL attributes set if nothing passed") {

      expecting {
        mockDseCqlStatement(gatlingSession).andReturn(new SimpleStatement("select * from test").success)
        mockDseSession.executeAsync(capture(statementCapture)) andReturn mock[ResultSetFuture]
      }

      whenExecuting(mockDseCqlStatement, mockDseSession) {
        getTarget(cqlAttributes).sendQuery(gatlingSession)
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

      val cqlAttributes = DseAttributes("test", mockDseCqlStatement,
        cl = Some(queryConsistencyLevel),
        checks = List.empty[DseCheck],
        userOrRole = Some(queryUser),
        readTimeout = Some(queryReadTimeout),
        defaultTimestamp = Some(queryDefaultTimestamp),
        idempotent = Some(queryIdempotent),

        // cql Specific
        cqlFetchSize = Some(fetchSize),
        cqlSerialCl = Some(lwtCl),
        cqlRetryPolicy = Some(retryPolicy),
        cqlEnableTrace = Some(true),
        //        cqlPagingState = Some(pagingState),
      )

      val statementCapture = EasyMock.newCapture[RegularStatement]

      expecting {
        mockDseCqlStatement(gatlingSession).andReturn(new SimpleStatement("select * from test").success)
        mockDseSession.executeAsync(capture(statementCapture)) andReturn mock[ResultSetFuture]
      }

      whenExecuting(mockDseCqlStatement, mockDseSession) {
        getTarget(cqlAttributes).sendQuery(gatlingSession)
      }

      val capturedStatement = statementCapture.getValue
      capturedStatement shouldBe a[SimpleStatement]
      // default attributes
      capturedStatement.getConsistencyLevel shouldBe queryConsistencyLevel
      capturedStatement.getDefaultTimestamp shouldBe queryDefaultTimestamp
      capturedStatement.getReadTimeoutMillis shouldBe queryReadTimeout
      capturedStatement.isIdempotent shouldBe queryIdempotent

      // cql attributes
      capturedStatement.getFetchSize shouldBe fetchSize
      capturedStatement.getSerialConsistencyLevel shouldBe lwtCl
      capturedStatement.getRetryPolicy shouldBe retryPolicy
      capturedStatement.getQueryString should be("select * from test")
      capturedStatement.isTracing shouldBe true
    }
  }

  describe("Graph") {

    val graphReadCL = ConsistencyLevel.LOCAL_QUORUM
    val graphWriteCL = ConsistencyLevel.LOCAL_QUORUM
    val graphName = "MyGraph"
    val graphLanguage = "english"
    val graphSource = "mysource"


    it("should enabled all the Graph Attributes in DseAttributes") {

      val graphAttributes = DseAttributes("test", mockDseGraphStatement,
        cl = Some(queryConsistencyLevel),
        checks = List.empty[DseCheck],
        userOrRole = Some(queryUser),
        readTimeout = Some(queryReadTimeout),
        defaultTimestamp = Some(queryDefaultTimestamp),
        idempotent = Some(queryIdempotent),

        graphReadCL = Some(graphReadCL),
        graphWriteCL = Some(graphWriteCL),
        graphName = Some(graphName),
        graphLanguage = Some(graphLanguage),
        graphSource = Some(graphSource),
        //        graphSystemQuery = Some(true),
        graphInternalOptions = Some(Seq(("get", "this"))),
        graphTransformResults = None
      )

      val statementCapture = EasyMock.newCapture[RegularGraphStatement]

      expecting {
        mockDseGraphStatement(gatlingSession).andReturn(new SimpleGraphStatement("g.V()").success)
        mockDseSession.executeGraphAsync(capture(statementCapture)) andReturn mock[ListenableFuture[GraphResultSet]]
      }

      whenExecuting(mockDseGraphStatement, mockDseSession) {
        getTarget(graphAttributes).sendQuery(gatlingSession)
      }

      val capturedStatement = statementCapture.getValue
      capturedStatement shouldBe a[SimpleGraphStatement]
      // default attributes
      capturedStatement.getConsistencyLevel shouldBe queryConsistencyLevel
      capturedStatement.getDefaultTimestamp shouldBe queryDefaultTimestamp
      capturedStatement.getReadTimeoutMillis shouldBe queryReadTimeout
      capturedStatement.isIdempotent shouldBe queryIdempotent

      // graph attributes
      capturedStatement.getGraphName shouldBe graphName
      capturedStatement.getGraphLanguage shouldBe graphLanguage
      capturedStatement.getGraphReadConsistencyLevel shouldBe graphReadCL
      capturedStatement.getGraphWriteConsistencyLevel shouldBe graphWriteCL
      capturedStatement.getGraphSource shouldBe graphSource
      capturedStatement.isSystemQuery shouldBe false
      capturedStatement.getGraphInternalOption("get") shouldBe "this"
    }

    it("should override the graph name if system") {

      val graphAttributes = DseAttributes("test", mockDseGraphStatement,
        graphName = Some(graphName),
        graphSystemQuery = Some(true),
      )

      val statementCapture = EasyMock.newCapture[RegularGraphStatement]

      expecting {
        mockDseGraphStatement(gatlingSession).andReturn(new SimpleGraphStatement("g.V()").success)
        mockDseSession.executeGraphAsync(capture(statementCapture)) andReturn mock[ListenableFuture[GraphResultSet]]
      }

      whenExecuting(mockDseGraphStatement, mockDseSession) {
        getTarget(graphAttributes).sendQuery(gatlingSession)
      }

      val capturedStatement = statementCapture.getValue
      capturedStatement shouldBe a[SimpleGraphStatement]
      // graph attributes
      capturedStatement.getGraphName shouldBe null
      capturedStatement.isSystemQuery shouldBe true
    }
  }
}


class CustomRetryPolicy(val readAttempts: Int, val writeAttempts: Int, val unavailableAttempts: Int) extends RetryPolicy {

  def init(cluster: Cluster) = {

  }

  def onReadTimeout(statement: Statement, cl: ConsistencyLevel, requiredResponses: Int, receivedResponses: Int, dataRetrieved: Boolean, nbRetry: Int) = {
    RetryDecision.rethrow
  }

  def onWriteTimeout(statement: Statement, cl: ConsistencyLevel, writeType: WriteType, requiredAcks: Int, receivedAcks: Int, nbRetry: Int): RetryDecision = {
    RetryDecision.rethrow
  }

  def onUnavailable(statement: Statement, cl: ConsistencyLevel, requiredReplica: Int, aliveReplica: Int, nbRetry: Int): RetryDecision = {
    RetryDecision.rethrow
  }

  override def onRequestError(statement: Statement, cl: ConsistencyLevel, e: DriverException, nbRetry: Int) = {
    RetryDecision.rethrow
  }

  override def close() = {
  }
}

