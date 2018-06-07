/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.request

import java.lang.System.{currentTimeMillis, nanoTime}
import java.util.UUID

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future, Promise}

import akka.actor.{ActorRef, ActorSystem}
import com.datastax.driver.core.Statement
import com.datastax.gatling.plugin.metrics.MetricsLogger
import com.datastax.gatling.plugin.response.CqlResponseHandler
import com.datastax.gatling.plugin.utils.ResponseTimers
import com.datastax.gatling.plugin.{DseCqlStatement, DseProtocol}
import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture, MoreExecutors}
import io.gatling.commons.stats.KO
import io.gatling.commons.validation.Validation
import io.gatling.core.action.{Action, ExitableAction}
import io.gatling.core.session.Session
import io.gatling.core.stats.StatsEngine

/**
  *
  * This class is responsible for executing CQL queries asynchronously against the cluster.
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
class DseCqlRequestAction(val name: String,
                          val next: Action,
                          val system: ActorSystem,
                          val statsEngine: StatsEngine,
                          val protocol: DseProtocol,
                          val dseAttributes: DseCqlAttributes,
                          val metricsLogger: MetricsLogger,
                          val dseRouter: ActorRef)
  extends ExitableAction {

  def execute(session: Session): Unit = {
    dseRouter ! SendCqlQuery(this, session)
  }

  def sendQuery(session: Session): Unit = {
    val stmt: Validation[Statement] = {
      dseAttributes.statement match {
        case cql: DseCqlStatement => cql.buildFromFeeders(session)
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

      // global options
      dseAttributes.cl.map(stmt.setConsistencyLevel)
      dseAttributes.userOrRole.map(stmt.executingAs)
      dseAttributes.readTimeout.map(stmt.setReadTimeoutMillis)
      dseAttributes.idempotent.map(stmt.setIdempotent)
      dseAttributes.defaultTimestamp.map(stmt.setDefaultTimestamp)

      // CQL Only Options
      dseAttributes.outGoingPayload.map(x => stmt.setOutgoingPayload(x.asJava))
      dseAttributes.serialCl.map(stmt.setSerialConsistencyLevel)
      dseAttributes.retryPolicy.map(stmt.setRetryPolicy)
      dseAttributes.fetchSize.map(stmt.setFetchSize)
      dseAttributes.pagingState.map(stmt.setPagingState)
      if (dseAttributes.enableTrace.isDefined && dseAttributes.enableTrace.get) {
        stmt.enableTracing
      }

      val responseHandler = new CqlResponseHandler(next, session, system, statsEngine, startTimes, stmt, dseAttributes, metricsLogger)
      implicit val sameThreadExecutionContext: ExecutionContextExecutor = ExecutionContext.fromExecutor(MoreExecutors.directExecutor())
      protocol.session
        .executeAsync(stmt)
        .onComplete(t => {
          dseRouter ! RecordResult(t, responseHandler)
        })
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
