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
import com.datastax.gatling.plugin.DseProtocol
import com.datastax.gatling.plugin.metrics.MetricsLogger
import com.datastax.gatling.plugin.model.DseGraphAttributes
import com.datastax.gatling.plugin.response.GraphResponseHandler
import com.datastax.gatling.plugin.utils._
import io.gatling.commons.stats.KO
import io.gatling.core.action.{Action, ExitableAction}
import io.gatling.core.session.Session
import io.gatling.core.stats.StatsEngine

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
class GraphRequestAction(val name: String,
                         val next: Action,
                         val system: ActorSystem,
                         val statsEngine: StatsEngine,
                         val protocol: DseProtocol,
                         val dseAttributes: DseGraphAttributes,
                         val metricsLogger: MetricsLogger,
                         val dseExecutorService: ExecutorService,
                         val gatlingTimingSource: GatlingTimingSource)
  extends ExitableAction {

  def execute(session: Session): Unit = {
    dseExecutorService.submit(new Runnable {
      override def run(): Unit = sendQuery(session)
    })
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

    stmt.onFailure(err => {
      val responseTime = responseTimeBuilder.build()
      val logUuid = UUID.randomUUID.toString
      val tagString = if (session.groupHierarchy.nonEmpty) session.groupHierarchy.mkString("/") + "/" + dseAttributes.tag else dseAttributes.tag

      statsEngine.logResponse(session, name, responseTime.toGatlingResponseTimings, KO, None,
        Some(s"$tagString - Preparing: ${err.take(50)}"), List(responseTime.latencyIn(MICROSECONDS), "PRE", logUuid))

      logger.error("[{}] {} - Preparing: {} - Attrs: {}", logUuid, tagString, err, session.attributes.mkString(","))
      next ! session.markAsFailed
    })

    stmt.onSuccess({ gStmt =>
      // global options
      dseAttributes.cl.map(gStmt.setConsistencyLevel)
      dseAttributes.defaultTimestamp.map(gStmt.setDefaultTimestamp)
      dseAttributes.userOrRole.map(gStmt.executingAs)
      dseAttributes.readTimeout.map(gStmt.setReadTimeoutMillis)
      dseAttributes.idempotent.map(gStmt.setIdempotent)

      // Graph only Options
      dseAttributes.readCL.map(gStmt.setGraphReadConsistencyLevel)
      dseAttributes.writeCL.map(gStmt.setGraphWriteConsistencyLevel)
      dseAttributes.graphLanguage.map(gStmt.setGraphLanguage)
      dseAttributes.graphName.map(gStmt.setGraphName)
      dseAttributes.graphSource.map(gStmt.setGraphSource)
      dseAttributes.graphTransformResults.map(gStmt.setTransformResultFunction)

      if (dseAttributes.graphInternalOptions.isDefined) {
        dseAttributes.graphInternalOptions.get.foreach { t =>
          gStmt.setGraphInternalOption(t._1, t._2)
        }
      }

      if (dseAttributes.isSystemQuery.isDefined && dseAttributes.isSystemQuery.get) {
        gStmt.setSystemQuery()
      }

      val responseHandler = new GraphResponseHandler(next, session, system, statsEngine, responseTimeBuilder, gStmt, dseAttributes, metricsLogger)
      implicit val sameThreadExecutionContext: ExecutionContextExecutor = ExecutionContext.fromExecutorService(dseExecutorService)
      FutureUtils
        .toScalaFuture(protocol.session.executeGraphAsync(gStmt))
        .onComplete(t => DseRequestActor.recordResult(RecordResult(t, responseHandler)))
    })
  }
}
