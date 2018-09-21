/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.request


import akka.actor.Actor
import com.google.common.util.concurrent.FutureCallback
import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.session.Session

import scala.concurrent.ExecutionException
import scala.util.{Failure, Success, Try}

case class SendCqlQuery(dseRequestAction: CqlRequestAction, session: Session)
case class SendGraphQuery(dseRequestAction: GraphRequestAction, session: Session)

case class RecordResult[T](t: Try[T], callback: FutureCallback[T])

class DseRequestActor extends Actor with StrictLogging {
  override def receive: Actor.Receive = {
    case SendCqlQuery(action, session) => action.sendQuery(session)
    case SendGraphQuery(action, session) => action.sendQuery(session)
    case r: RecordResult[_] => DseRequestActor.recordResult(r)
  }
}

object DseRequestActor extends StrictLogging {
  def recordResult[T](result: RecordResult[T]): Unit = result match {
    case RecordResult(t, callback) => t match {
      case Success(resultSet) => callback.onSuccess(resultSet)
      case Failure(exception: ExecutionException) => callback.onFailure(exception.getCause)
      case Failure(exception: Exception) => callback.onFailure(exception)
      case Failure(exception: Throwable) =>
        logger.error("Caught an unexpected error, please file a ticket", exception)
        callback.onFailure(exception)
    }
  }
}