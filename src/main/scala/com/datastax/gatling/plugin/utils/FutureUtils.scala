package com.datastax.gatling.plugin.utils

import com.google.common.util.concurrent.{FutureCallback, Futures, ListenableFuture}

import scala.concurrent.{Future, Promise}

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
  def toScalaFuture[T](guavaFuture: ListenableFuture[T]): Future[T] = {
    val scalaPromise = Promise[T]()
    Futures.addCallback(
      guavaFuture,
      new FutureCallback[T] {
        def onSuccess(result: T): Unit = scalaPromise.success(result)

        def onFailure(exception: Throwable): Unit = scalaPromise.failure(exception)
      }
    )
    scalaPromise.future
  }
}
