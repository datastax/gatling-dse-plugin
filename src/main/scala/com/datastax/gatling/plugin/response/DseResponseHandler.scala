/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.response

import java.util.UUID
import java.util.concurrent.TimeUnit.MICROSECONDS

import akka.actor.ActorSystem
import com.datastax.driver.core._
import com.datastax.driver.dse.graph.{GraphProtocol, GraphResultSet, GraphStatement}
import com.datastax.gatling.plugin.metrics.MetricsLogger
import com.datastax.gatling.plugin.model.{DseCqlAttributes, DseGraphAttributes}
import com.datastax.gatling.plugin.utils.{ResponseTime, ResponseTimeBuilder}
import com.google.common.util.concurrent.FutureCallback
import com.typesafe.scalalogging.StrictLogging
import io.gatling.commons.stats._
import io.gatling.commons.validation.Failure
import io.gatling.core.action.Action
import io.gatling.core.check.Check
import io.gatling.core.session.Session
import io.gatling.core.stats.StatsEngine
import io.gatling.core.stats.message.ResponseTimings

import scala.util.Try

object DseResponseHandler {
  def sanitize(s: String): String = s.replaceAll("""(\r|\n)""", " ")

  def sanitizeAndJoin(statements: Seq[String]): String = statements
    .map(s => sanitize(s))
    .mkString(",")
}

abstract class DseResponseHandler[RS, Response <: DseResponse] extends StrictLogging with FutureCallback[RS] {
  protected def responseTimeBuilder: ResponseTimeBuilder
  protected def system: ActorSystem
  protected def statsEngine: StatsEngine
  protected def metricsLogger: MetricsLogger
  protected def next: Action
  protected def session: Session
  protected def stmt: Any
  protected def tag: String
  protected def queries: Seq[String]
  protected def specificChecks: List[Check[Response]]
  protected def genericChecks: List[Check[DseResponse]]
  protected def newResponse(rs: RS): Response
  protected def queriedHost(rs: RS): String

  private def writeGatlingLog(status: Status, respTimings: ResponseTimings, message: Option[String], extraInfo: List[Any]): Unit =
    statsEngine.logResponse(session, tag, respTimings, status, None, message, extraInfo)

  protected def writeSuccess(responseTime: ResponseTime): Unit = {
    metricsLogger.log(session, tag, responseTime, ok = true)
    writeGatlingLog(OK, responseTime.toGatlingResponseTimings, None, List(responseTime.latencyIn(MICROSECONDS), "", ""))
  }

  protected def writeCheckFailure(checkRes: (Session => Session, Option[Failure]), resultSet: RS, responseTime: ResponseTime): Unit = {
    metricsLogger.log(session, tag, responseTime, ok = false)

    val logUuid = UUID.randomUUID.toString
    val tagString = if (session.groupHierarchy.nonEmpty) session.groupHierarchy.mkString("/") + "/" + tag else tag

    writeGatlingLog(
      KO, responseTime.toGatlingResponseTimings,
      Some(s"$tagString - Check: ${checkRes._2.get.message.take(50)}"),
      List(responseTime.latencyIn(MICROSECONDS), "CHK", logUuid)
    )

    logger.warn("[{}] {} - Check: {}, Query: {}, Host: {}",
      logUuid, tagString, checkRes._2.get.message, DseResponseHandler.sanitizeAndJoin(queries), queriedHost(resultSet)
    )
  }

  protected def writeFailure(t: Throwable, responseTime: ResponseTime): Unit = {

    metricsLogger.log(session, tag, responseTime, ok = false)

    val logUuid = UUID.randomUUID.toString
    val tagString = if (session.groupHierarchy.nonEmpty) session.groupHierarchy.mkString("/") + "/" + tag else tag

    writeGatlingLog(KO, responseTime.toGatlingResponseTimings,
      Some(s"$tagString - Execute: ${t.getClass.getSimpleName}"),
      List(responseTime.latencyIn(MICROSECONDS), "CHK", logUuid)
    )

    stmt match {
      case Some(gs: GraphStatement) =>
        val unwrap = Try(
          DseResponseHandler.sanitize(gs.unwrap(GraphProtocol.GRAPHSON_2_0).toString)
        ).getOrElse(DseResponseHandler.sanitize(gs.unwrap(GraphProtocol.GRAPHSON_1_0).toString))

        logger.warn("[{}] {} - Execute: {} - Attrs: {}",
          logUuid, tagString, unwrap, session.attributes.mkString(","), t
        )
      case _ =>
        logger.warn("[{}] {} - Execute: {}, Query: {}",
          logUuid, tagString, t.getMessage, DseResponseHandler.sanitizeAndJoin(queries)
        )
        logger.debug("Complete exception:", t)
    }

  }

  override def onFailure(t: Throwable): Unit = {
    writeFailure(t, responseTimeBuilder.build())
    next ! session.markAsFailed
  }

  override def onSuccess(result: RS): Unit = {
    val responseTime = responseTimeBuilder.build()
    val response = newResponse(result)

    val genericResult: (Session => Session, Option[Failure]) = Check.check(response, session, genericChecks)
    val genericChecksPassed = genericResult._2.isEmpty
    val sessionAfterGenericChecks = genericResult._1(session)
    if (genericChecksPassed) {
      val specificResult: (Session => Session, Option[Failure]) = Check.check(response, sessionAfterGenericChecks, specificChecks)
      val sessionAfterSpecificChecks = genericResult._1(session)
      val specificChecksPassed = specificResult._2.isEmpty
      if (specificChecksPassed) {
        writeSuccess(responseTime)
        next ! sessionAfterSpecificChecks.markAsSucceeded
      } else {
        writeCheckFailure(specificResult, result, responseTime)
        next ! sessionAfterSpecificChecks.markAsFailed
      }
    } else {
      // Do not run specific checks as the response is already error'ed
      writeCheckFailure(genericResult, result, responseTime)
      next ! sessionAfterGenericChecks.markAsFailed
    }
  }
}

class GraphResponseHandler(val next: Action,
                           val session: Session,
                           val system: ActorSystem,
                           val statsEngine: StatsEngine,
                           val responseTimeBuilder: ResponseTimeBuilder,
                           val stmt: GraphStatement,
                           val dseAttributes: DseGraphAttributes,
                           val metricsLogger: MetricsLogger)
    extends DseResponseHandler[GraphResultSet, GraphResponse] {
  override protected def tag: String = dseAttributes.tag
  override protected def queries: Seq[String] = Seq.empty
  override protected def specificChecks: List[Check[GraphResponse]] = dseAttributes.graphChecks
  override protected def genericChecks: List[Check[DseResponse]] = dseAttributes.genericChecks
  override protected def newResponse(rs: GraphResultSet): GraphResponse = new GraphResponse(rs, dseAttributes)
  override protected def queriedHost(rs: GraphResultSet): String = rs.getExecutionInfo.getQueriedHost.toString
}

class CqlResponseHandler(val next: Action,
                         val session: Session,
                         val system: ActorSystem,
                         val statsEngine: StatsEngine,
                         val responseTimeBuilder: ResponseTimeBuilder,
                         val stmt: Statement,
                         val dseAttributes: DseCqlAttributes,
                         val metricsLogger: MetricsLogger)
  extends DseResponseHandler[ResultSet, CqlResponse] {
  override protected def tag: String = dseAttributes.tag
  override protected def queries: Seq[String] = Seq.empty
  override protected def specificChecks: List[Check[CqlResponse]] = dseAttributes.cqlChecks
  override protected def genericChecks: List[Check[DseResponse]] = dseAttributes.genericChecks
  override protected def newResponse(rs: ResultSet): CqlResponse = new CqlResponse(rs, dseAttributes)
  override protected def queriedHost(rs: ResultSet): String = rs.getExecutionInfo.getQueriedHost.toString
}
