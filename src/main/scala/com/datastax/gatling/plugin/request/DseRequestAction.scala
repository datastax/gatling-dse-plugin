package com.datastax.gatling.plugin.request

import java.lang.System.{currentTimeMillis, nanoTime}
import java.util.UUID
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import akka.actor.ActorSystem
import com.datastax.driver.core.{ResultSet, Statement}
import com.datastax.driver.dse.graph.{GraphResultSet, GraphStatement}
import com.datastax.gatling.plugin.metrics.HistogramLogger
import com.datastax.gatling.plugin.response.{CqlResponseHandler, GraphResponseHandler}
import com.datastax.gatling.plugin.utils.ResponseTimers
import com.datastax.gatling.plugin.{DseCqlStatement, DseGraphStatement, DseProtocol}
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture, MoreExecutors}
import io.gatling.commons.stats.KO
import io.gatling.commons.validation.Validation
import io.gatling.core.action.{Action, ExitableAction}
import io.gatling.core.session.Session
import io.gatling.core.stats.StatsEngine

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

class DseRequestAction(val name: String, val next: Action, val system: ActorSystem,
                       val statsEngine: StatsEngine, protocol: DseProtocol, dseAttributes: DseAttributes,
                       histogramLogger: HistogramLogger, timeoutEnforcer: ScheduledExecutorService) extends ExitableAction {

  /**
    * Execute the CQL or GraphStatement asynchronously against the cluster
    *
    * @param session Gatling Session
    */
  def execute(session: Session): Unit = {

    val stmt: Validation[Any] = {
      dseAttributes.statement match {
        case cql: DseCqlStatement => cql(session)
        case graph: DseGraphStatement => graph(session)
      }
    }

    stmt.onFailure(err => {
      val respTimings = ResponseTimers((nanoTime, currentTimeMillis))
      val logUuid = UUID.randomUUID.toString
      val tagString = if (session.groupHierarchy.nonEmpty) session.groupHierarchy.mkString("/") + "/" + dseAttributes.tag else dseAttributes.tag

      statsEngine.logResponse(session, name, respTimings.responseTimings, KO, None,
        Some(s"$tagString - Preparing: ${err.take(50)}"), List(respTimings.diffMicros, "PRE", logUuid))

      logger.error("[{}] {} - Preparing: {} - Attrs: {}", logUuid, tagString, err, session.attributes.mkString(","))
      next ! session.markAsFailed
    })

    stmt.onSuccess({ stmt =>
      val startTimes = (nanoTime, currentTimeMillis)

      stmt match {

        case stmt: Statement =>
          // global options
          dseAttributes.cl.map(stmt.setConsistencyLevel)
          dseAttributes.userOrRole.map(stmt.executingAs)
          dseAttributes.readTimeout.map(stmt.setReadTimeoutMillis)
          dseAttributes.idempotent.map(stmt.setIdempotent)
          dseAttributes.defaultTimestamp.map(stmt.setDefaultTimestamp)

          // CQL Only Options
          dseAttributes.cqlOutGoingPayload.map(x => stmt.setOutgoingPayload(x.asJava))
          dseAttributes.cqlSerialCl.map(stmt.setSerialConsistencyLevel)
          dseAttributes.cqlRetryPolicy.map(stmt.setRetryPolicy)
          dseAttributes.cqlFetchSize.map(stmt.setFetchSize)
          dseAttributes.cqlPagingState.map(stmt.setPagingState)
          if (dseAttributes.cqlEnableTrace.isDefined && dseAttributes.cqlEnableTrace.get) {
            stmt.enableTracing
          }

          wrapWithTimeout(
            protocol.session.executeAsync(stmt),
            new CqlResponseHandler(next, session, system, statsEngine, startTimes, stmt, dseAttributes, histogramLogger))
        case gStmt: GraphStatement =>
          // global options
          dseAttributes.cl.map(gStmt.setConsistencyLevel)
          dseAttributes.defaultTimestamp.map(gStmt.setDefaultTimestamp)
          dseAttributes.userOrRole.map(gStmt.executingAs)
          dseAttributes.readTimeout.map(gStmt.setReadTimeoutMillis)
          dseAttributes.idempotent.map(gStmt.setIdempotent)

          // Graph only Options
          dseAttributes.graphReadCL.map(gStmt.setGraphReadConsistencyLevel)
          dseAttributes.graphWriteCL.map(gStmt.setGraphWriteConsistencyLevel)
          dseAttributes.graphLanguage.map(gStmt.setGraphLanguage)
          dseAttributes.graphName.map(gStmt.setGraphName)
          dseAttributes.graphSource.map(gStmt.setGraphSource)
          dseAttributes.graphTransformResults.map(gStmt.setTransformResultFunction)

          if (dseAttributes.graphInternalOptions.isDefined) {
            dseAttributes.graphInternalOptions.get.foreach { t =>
              gStmt.setGraphInternalOption(t._1, t._2)
            }
          }

          if (dseAttributes.graphSystemQuery.isDefined && dseAttributes.graphSystemQuery.get) {
            gStmt.setSystemQuery()
          }

          wrapWithTimeout(
            protocol.session.executeGraphAsync(gStmt),
            new GraphResponseHandler(next, session, system, statsEngine, startTimes, gStmt, dseAttributes, histogramLogger))
      }
    })
  }

  /*
  In previous versions of the plugin, logging the results in HDR histograms was made by the driver Netty I/O thread.
  This was a bad practice as per the java-driver documentation.  Expensive operations should be delegated.
  Since response timing logging involved lock, the Netty I/O thread was sometimes blocked.  Thus, it could not send new
  queries or fetch response properly.

  The new workflow is as follows:
  1. A Gatling `user` actor sends a request to DSE using `.executeAsync()` or `.executeGraphAsync()`.
  2. The driver's I/O thread (Netty) completes the `ResultSetFuture` that was previously returned.
     While doing so, it transforms that Guava future into a Scala future, which is a very fast operation
  3. That future is then handed off to a dedicated Akka actor which records the outcome (success/error) asynchronously
     It has the effect of immediately freeing the Netty I/O thread, thus allowing it to process other queries/answers.

  This code also includes a fail-safe timeout.  In long running fallout tests, several queries never completed, even
  with a timeout.  That issue has not been diagnosed yet.  To work around it, now each query is monitored and killed
  after 1 minute.  That timeout is hard coded for now.

  TODO Make the fail-safe timeout configurable
  */
  private def wrapWithTimeout[T](futureResult: ListenableFuture[T], responseHandler: FutureCallback[T]): Unit = {
    implicit val executionContext: ExecutionContext = DseRequestAction.sameThreadExecutionContext
    Futures
      .withTimeout(futureResult, 60, TimeUnit.SECONDS, timeoutEnforcer)
      .onComplete((t: Try[T]) => histogramLogger.logAsync(t, responseHandler))
  }

  private implicit def guavaFutureToScalaFuture[T](guavaFuture: ListenableFuture[T]): Future[T] = {
    val scalaPromise = Promise[T]()
    Futures.addCallback(guavaFuture,
      new FutureCallback[T] {
        def onSuccess(result: T): Unit = scalaPromise.success(result)

        def onFailure(exception: Throwable): Unit = scalaPromise.failure(exception)
      })
    scalaPromise.future
  }
}

object DseRequestAction {
  def sameThreadExecutionContext: ExecutionContext = ExecutionContext.fromExecutor(MoreExecutors.directExecutor())
}
