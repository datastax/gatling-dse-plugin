/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.utils


import java.util.concurrent.{CompletionStage}

import scala.concurrent.java8.FuturesConvertersImpl._
import scala.concurrent.{Future}

object FutureUtils {
  /**
    * Converts a Guava future into a Scala future containing the exact same
    * result.
    *
    * The conversion is operated by the same thread that will eventually
    * complete the guava future.  It is not an expensive operation, though it
    * may involve CAS operations under the hood.
    *
    * @param guavaFuture the Guava future to convert
    * @tparam T the type of object returned by the Guava future
    * @return a Scala [[Future]] that will be completed when the Guava future
    *         completes
    */
  def toScalaFuture[T](guavaFuture: CompletionStage[T]): Future[T] = {
    guavaFuture match {
      case cf: CF[T] => cf.wrapped
      case _ =>
        val p = new P[T](guavaFuture)
        guavaFuture whenComplete p
        p.future
    }
  }
}
