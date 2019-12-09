/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.request

import java.lang.Boolean
import java.util.UUID
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit.MICROSECONDS

import akka.actor.ActorSystem
import com.datastax.dse.driver.api.core.graph.{GraphStatement, GraphStatementBuilderBase}
import com.datastax.gatling.plugin.DseProtocol
import com.datastax.gatling.plugin.metrics.MetricsLogger
import com.datastax.gatling.plugin.model.DseGraphAttributes
import com.datastax.gatling.plugin.response.GraphResponseHandler
import com.datastax.gatling.plugin.utils._
import io.gatling.commons.stats.KO
import io.gatling.core.action.{Action, ExitableAction}
import io.gatling.core.session.Session
import io.gatling.core.stats.StatsEngine

import scala.compat.java8.FutureConverters
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

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
class GraphRequestAction[T <: GraphStatement[T]](val name: String,
                         val next: Action,
                         val system: ActorSystem,
                         val statsEngine: StatsEngine,
                         val protocol: DseProtocol,
                         val dseAttributes: DseGraphAttributes[T],
                         val metricsLogger: MetricsLogger,
                         val dseExecutorService: ExecutorService,
                         val gatlingTimingSource: GatlingTimingSource)
  extends ExitableAction {

  def execute(session: Session): Unit = {
    dseExecutorService.submit(new Runnable {
      override def run(): Unit = sendQuery(session)
    })
  }

  private def buildStatement(builder:GraphStatementBuilderBase[_,T]):T = {

    // global options
    dseAttributes.cl.map(builder.setConsistencyLevel)
    dseAttributes.idempotent.foreach(builder.setIdempotence(_))
    dseAttributes.node.foreach(builder.setNode)

    // Graph only Options
    dseAttributes.graphName.foreach(builder.setGraphName)
    dseAttributes.readCL.foreach(builder.setReadConsistencyLevel)
    dseAttributes.subProtocol.foreach(builder.setSubProtocol)
    dseAttributes.timeout.foreach(builder.setTimeout)
    dseAttributes.timestamp.foreach(builder.setTimestamp)
    dseAttributes.traversalSource.foreach(builder.setTraversalSource)
    dseAttributes.writeCL.foreach(builder.setWriteConsistencyLevel)

    builder.build
  }

  private def handleSuccess(session: Session, responseTimeBuilder: ResponseTimeBuilder)(builder:GraphStatementBuilderBase[_,T]): Unit = {

    val stmt:T = buildStatement(builder)
    val responseHandler =
      new GraphResponseHandler[T](
        next,
        session,
        system,
        statsEngine,
        responseTimeBuilder,
        stmt,
        dseAttributes,
        metricsLogger)
    implicit val sameThreadExecutionContext: ExecutionContextExecutor = ExecutionContext.fromExecutorService(dseExecutorService)
    FutureConverters
      .toScala(protocol.session.executeAsync(stmt))
      .onComplete(t => DseRequestActor.recordResult(RecordResult(t, responseHandler)))
  }

  private def handleFailure(session: Session, responseTimeBuilder: ResponseTimeBuilder)(err: String) = {

    val responseTime = responseTimeBuilder.build()
    val logUuid = UUID.randomUUID.toString
    val tagString = if (session.groupHierarchy.nonEmpty) session.groupHierarchy.mkString("/") + "/" + dseAttributes.tag else dseAttributes.tag

    statsEngine.logResponse(session, name, responseTime.toGatlingResponseTimings, KO, None,
      Some(s"$tagString - Preparing: ${err.take(50)}"), List(responseTime.latencyIn(MICROSECONDS), "PRE", logUuid))

    logger.error("[{}] {} - Preparing: {} - Attrs: {}", logUuid, tagString, err, session.attributes.mkString(","))
    next ! session.markAsFailed
  }

  def sendQuery(session: Session): Unit = {
    val enableCO = Boolean.getBoolean("gatling.dse.plugin.measure_service_time")
    val responseTimeBuilder: ResponseTimeBuilder = if (enableCO) {
      // The throughput checker is useless in CO affected scenarios since throughput is not known in advance
      COAffectedResponseTime.startingNow(gatlingTimingSource)
    } else {
      ThroughputVerifier.checkForGatlingOverloading(session, gatlingTimingSource)
      GatlingResponseTime.startedByGatling(session, gatlingTimingSource)
    }
    val stmt = dseAttributes.statement.buildFromSession(session)
    stmt.onFailure(handleFailure(session, responseTimeBuilder))
    stmt.onSuccess(handleSuccess(session, responseTimeBuilder))
  }
}
