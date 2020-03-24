/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.response

import java.util.UUID
import java.util.concurrent.TimeUnit.MICROSECONDS

import akka.actor.ActorSystem
import com.datastax.oss.driver.api.core.cql._
import com.datastax.dse.driver.api.core.graph.{AsyncGraphResultSet, GraphStatement, GraphStatementBuilderBase}
import com.datastax.gatling.plugin.checks.{DseCqlCheck, DseGraphCheck}
import com.datastax.gatling.plugin.metrics.MetricsLogger
import com.datastax.gatling.plugin.model.{DseCqlAttributes, DseGraphAttributes}
import com.datastax.gatling.plugin.utils.{ResponseTime, ResponseTimeBuilder}
import com.datastax.oss.driver.api.core.metadata.Node
import com.typesafe.scalalogging.StrictLogging
import io.gatling.commons.stats._
import io.gatling.commons.validation.Failure
import io.gatling.core.action.Action
import io.gatling.core.check.Check
import io.gatling.core.session.Session
import io.gatling.core.stats.StatsEngine
import io.gatling.core.stats.message.ResponseTimings

import collection.JavaConverters._

object DseResponseHandler {
  def sanitize(s: String): String = s.replaceAll("""(\r|\n)""", " ")

  def sanitizeAndJoin(statements: Seq[String]): String = statements
    .map(s => sanitize(s))
    .mkString(",")
}

trait DseResponseCallback[RS] {
  def onFailure(t: Throwable): Unit

  def onSuccess(result: RS): Unit
}

abstract class DseResponseHandler[S, RS, R] extends StrictLogging with DseResponseCallback[RS] {
  protected def responseTimeBuilder: ResponseTimeBuilder
  protected def system: ActorSystem
  protected def statsEngine: StatsEngine
  protected def metricsLogger: MetricsLogger
  protected def next: Action
  protected def session: Session
  protected def stmt: S
  protected def tag: String
  protected def queries: Seq[String]
  protected def specificChecks: List[Check[R]]
  protected def newResponse(rs: RS): R
  protected def coordinator(rs: RS): Node

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

    logger.warn("[{}] {} - Check: {}, Query: {}, Coordinator: {}",
      logUuid, tagString, checkRes._2.get.message, DseResponseHandler.sanitizeAndJoin(queries), coordinator(resultSet).toString
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
      case Some(gs: GraphStatement[_]) =>
        logger.warn("[{}] {} - Execute: {} - Attrs: {}",
          logUuid, tagString, gs, session.attributes.mkString(","), t
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

    val specificResult: (Session => Session, Option[Failure]) = Check.check(response, session, specificChecks)
    val sessionAfterSpecificChecks = specificResult._1(session)
    val specificChecksPassed = specificResult._2.isEmpty
    if (specificChecksPassed) {
      writeSuccess(responseTime)
      next ! sessionAfterSpecificChecks.markAsSucceeded
    } else {
      writeCheckFailure(specificResult, result, responseTime)
      next ! sessionAfterSpecificChecks.markAsFailed
    }
  }
}

class GraphResponseHandler[T <: GraphStatement[T], B <: GraphStatementBuilderBase[B,T]](val next: Action,
                           val session: Session,
                           val system: ActorSystem,
                           val statsEngine: StatsEngine,
                           val responseTimeBuilder: ResponseTimeBuilder,
                           val stmt: T,
                           val dseAttributes: DseGraphAttributes[T, B],
                           val metricsLogger: MetricsLogger)
  extends DseResponseHandler[T, AsyncGraphResultSet, GraphResponse] {
  override protected def tag: String = dseAttributes.tag
  override protected def queries: Seq[String] = Seq.empty
  override protected def specificChecks: List[DseGraphCheck] = dseAttributes.graphChecks
  override protected def newResponse(rs: AsyncGraphResultSet): GraphResponse = new GraphResponse(rs, dseAttributes)
  override protected def coordinator(rs: AsyncGraphResultSet): Node = rs.getExecutionInfo.getCoordinator
}

class CqlResponseHandler[T <: Statement[T], B <: StatementBuilder[B,T]](val next: Action,
                         val session: Session,
                         val system: ActorSystem,
                         val statsEngine: StatsEngine,
                         val responseTimeBuilder: ResponseTimeBuilder,
                         val stmt: T,
                         val dseAttributes: DseCqlAttributes[T, B],
                         val metricsLogger: MetricsLogger)
  extends DseResponseHandler[T, AsyncResultSet, CqlResponse] {
  override protected def tag: String = dseAttributes.tag
  override protected def queries: Seq[String] = getQueryStrings(stmt)
  override protected def specificChecks: List[DseCqlCheck] = dseAttributes.cqlChecks
  override protected def newResponse(rs: AsyncResultSet): CqlResponse = new CqlResponse(rs, dseAttributes)
  override protected def coordinator(rs: AsyncResultSet): Node = rs.getExecutionInfo.getCoordinator

  def getQueryString(s:SimpleStatement):String = s.getQuery

  def getQueryString(s:BoundStatement):String = s.getPreparedStatement.getQuery

  def getQueryStrings(stmt:Statement[T]):Seq[String] = {

    stmt match {
      case s:SimpleStatement => Seq(getQueryString(s))
      case s:BoundStatement => Seq(getQueryString(s))
      case s:BatchStatement => s.iterator.asScala.map((stmt) => {
        stmt match {
          case s:SimpleStatement => getQueryString(s)
          case s:BoundStatement => getQueryString(s)
        }
      }).toSeq
    }
  }
}
