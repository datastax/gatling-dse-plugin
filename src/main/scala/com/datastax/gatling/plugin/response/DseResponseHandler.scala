/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.response

import java.util.UUID

import akka.actor.ActorSystem
import com.datastax.driver.core._
import com.datastax.driver.dse.graph.{GraphProtocol, GraphResultSet, GraphStatement}
import com.datastax.gatling.plugin.metrics.{HistogramLogger, MetricsLogger}
import com.datastax.gatling.plugin.request.DseAttributes
import com.datastax.gatling.plugin.utils.ResponseTimers
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


abstract class DseResponseHandler(next: Action, session: Session, system: ActorSystem, statsEngine: StatsEngine,
                                  startTimes: (Long, Long), stmt: Any, dseAttributes: DseAttributes,
                                  metricsLogger: MetricsLogger) extends StrictLogging {

  private def removeNewLineChars(str: String) = str.replaceAll("""(\r|\n)""", " ")

  private def writeGatlingLog(status: Status, respTimings: ResponseTimings, message: Option[String], extraInfo: List[Any]): Unit =
    statsEngine.logResponse(session, dseAttributes.tag, respTimings, status, None, message, extraInfo)

  protected def writeSuccess(respTimers: ResponseTimers): Unit = {
    metricsLogger.log(session, dseAttributes.tag, respTimers, ok = true)
    writeGatlingLog(OK, respTimers.responseTimings, None, List(respTimers.diffMicros, "", ""))
  }

  protected def writeCheckFailure(respTimers: ResponseTimers, checkRes: ((Session) => Session, Option[Failure]), resultSet: Any): Unit = {
    metricsLogger.log(session, dseAttributes.tag, respTimers, ok = false)

    val logUuid = UUID.randomUUID.toString
    val tagString = if (session.groupHierarchy.nonEmpty) session.groupHierarchy.mkString("/") + "/" + dseAttributes.tag else dseAttributes.tag

    writeGatlingLog(
      KO, respTimers.responseTimings,
      Some(s"$tagString - Check: ${checkRes._2.get.message.take(50)}"),
      List(respTimers.diffMicros, "CHK", logUuid)
    )

    val executionInfo: ExecutionInfo = {
      resultSet match {
        case rs: ResultSet => rs.getExecutionInfo
        case gr: GraphResultSet => gr.getExecutionInfo
      }
    }

    logger.warn("[{}] {} - Check: {}, Query: {}, Host: {}",
      logUuid, tagString, checkRes._2.get.message, getCqlQueriesAsString(dseAttributes), executionInfo.getQueriedHost.toString
    )
  }

  protected def writeFailure(respTimers: ResponseTimers, t: Throwable): Unit = {

    metricsLogger.log(session, dseAttributes.tag, respTimers, ok = false)

    val logUuid = UUID.randomUUID.toString
    val tagString = if (session.groupHierarchy.nonEmpty) session.groupHierarchy.mkString("/") + "/" + dseAttributes.tag else dseAttributes.tag

    writeGatlingLog(KO, respTimers.responseTimings,
      Some(s"$tagString - Execute: ${removeNewLineChars(t.getClass.getSimpleName)}"),
      List(respTimers.diffMicros, "CHK", logUuid)
    )

    stmt match {
      case Some(gs: GraphStatement) =>
        val unwrap = Try(
          removeNewLineChars(gs.unwrap(GraphProtocol.GRAPHSON_2_0).toString)
        ).getOrElse(removeNewLineChars(gs.unwrap(GraphProtocol.GRAPHSON_1_0).toString))

        logger.warn("[{}] {} - Execute: {} - Attrs: {}",
          logUuid, tagString, unwrap, session.attributes.mkString(","), t
        )
      case _ =>
        logger.warn("[{}] {} - Execute: {}, Query: {}",
          logUuid, tagString, removeNewLineChars(t.getMessage), getCqlQueriesAsString(dseAttributes)
        )
    }

  }

  protected def getCqlQueriesAsString(dseAttributes: DseAttributes): String = {
    removeNewLineChars(dseAttributes.cqlStatements.mkString(","))
  }

  def success(resultSet: Any): Unit = {
    val respTimers = ResponseTimers(startTimes)
    val checkRes = Check.check(new DseResponse(resultSet), session, dseAttributes.checks)

    if (checkRes._2.isEmpty) {
      writeSuccess(respTimers)
      next ! checkRes._1(session).markAsSucceeded
    } else {
      writeCheckFailure(respTimers, checkRes, resultSet)
      next ! checkRes._1(session).markAsFailed
    }
  }

  def failure(t: Throwable): Unit = {
    writeFailure(ResponseTimers(startTimes), t)
    next ! session.markAsFailed
  }
}


class GraphResponseHandler(next: Action, session: Session, system: ActorSystem, statsEngine: StatsEngine,
                           startTimes: (Long, Long), stmt: GraphStatement, dseAttributes: DseAttributes,
                           metricsLogger: MetricsLogger)
    extends DseResponseHandler(next, session, system, statsEngine, startTimes, stmt, dseAttributes, metricsLogger)
        with FutureCallback[GraphResultSet] {

  def onSuccess(resultSet: GraphResultSet): Unit = super.success(resultSet)

  def onFailure(t: Throwable): Unit = super.failure(t)

}

class CqlResponseHandler(next: Action, session: Session, system: ActorSystem, statsEngine: StatsEngine,
                         startTimes: (Long, Long), stmt: Statement, dseAttributes: DseAttributes,
                         metricsLogger: MetricsLogger)
    extends DseResponseHandler(next, session, system, statsEngine, startTimes, stmt, dseAttributes, metricsLogger)
        with FutureCallback[ResultSet] {

  def onSuccess(resultSet: ResultSet): Unit = super.success(resultSet)

  def onFailure(t: Throwable): Unit = super.failure(t)

}
