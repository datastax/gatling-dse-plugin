package com.datastax.gatling.plugin.base

import java.util.concurrent.{ExecutorService, Executors, ThreadFactory}

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.easymock.EasyMockSugar
import org.scalatest.{Tag, _}

abstract class BaseSpec extends FunSpec with Matchers with EasyMockSugar with BeforeAndAfter with BeforeAndAfterAll
    with LazyLogging {

  object CqlTest extends Tag("com.datastax.plugin.tags.CqlTest")

  object GraphTest extends Tag("com.datastax.plugin.tags.GraphTest")

  def executorServiceForTests():ExecutorService = Executors.newCachedThreadPool(new ThreadFactory {
    override def newThread(r: Runnable): Thread = {
      val thread = new Thread(r)
      thread.setDaemon(true)
      thread
    }
  })
}
