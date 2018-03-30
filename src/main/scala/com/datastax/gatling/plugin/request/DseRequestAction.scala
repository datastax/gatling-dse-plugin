/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.request

import java.lang.System.{currentTimeMillis, nanoTime}
import java.util.UUID

import akka.actor.{ActorRef, ActorSystem}
import com.datastax.driver.core.Statement
import com.datastax.driver.dse.graph.GraphStatement
import com.datastax.gatling.plugin.metrics.MetricsLogger
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
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future, Promise}

/**
  *
  * This class is responsible for executing CQL or Graph queries asynchronously against the cluster.
  *
  * It first starts by delegating everything that is driver related to the DSE plugin router.  This is in order to
  * free the Gatling `injector` actor as fast as possible.
  *
  * The plugin router (and its actors) execute the driver code in order to locate the best replica that should receive
  * each query, through `DseSession.executeAsync()` or `DseSession.executeGraphAsync()`.  A driver I/O thread encodes
  * the request and sends it over the wire.
  *
  * Once the response is received, a driver I/O thread (Netty) decodes the response into a Java Object.  It then
  * completes the `Future` that was returned by `DseSession.executeAsync()`.
  *
  * Completing that future results in immediately delegating the latency recording work to the plugin router.  That
  * work includes recording it in HDR histograms through non-blocking data structures, and forwarding the result to
  * other Gatling data writers, like the console reporter.
  */
class DseRequestAction(val name: String,
                       val next: Action,
                       val system: ActorSystem,
                       val statsEngine: StatsEngine,
                       val protocol: DseProtocol,
                       val dseAttributes: DseAttributes,
                       val metricsLogger: MetricsLogger,
                       val dseRouter: ActorRef)
  extends ExitableAction {

  def execute(session: Session): Unit = {
    dseRouter ! SendQuery(this, session)
  }

  def sendQuery(session: Session): Unit = {
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

          val responseHandler = new CqlResponseHandler(next, session, system, statsEngine, startTimes, stmt, dseAttributes, metricsLogger)
          implicit val sameThreadExecutionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(MoreExecutors.directExecutor())
          protocol.session
            .executeAsync(stmt)
            .onComplete(t => {dseRouter ! RecordResult(t, responseHandler)})
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

          val responseHandler = new GraphResponseHandler(next, session, system, statsEngine, startTimes, gStmt, dseAttributes, metricsLogger)
          implicit val sameThreadExecutionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(MoreExecutors.directExecutor())
          protocol.session
            .executeGraphAsync(gStmt)
            .onComplete(t => {dseRouter ! RecordResult(t, responseHandler)})
      }
    })
  }

  private implicit def toScalaFuture[T](guavaFuture: ListenableFuture[T]): Future[T] = {
    val scalaPromise = Promise[T]()
    Futures.addCallback(guavaFuture,
      new FutureCallback[T] {
        def onSuccess(result: T): Unit = scalaPromise.success(result)

        def onFailure(exception: Throwable): Unit = scalaPromise.failure(exception)
      })
    scalaPromise.future
  }
}
