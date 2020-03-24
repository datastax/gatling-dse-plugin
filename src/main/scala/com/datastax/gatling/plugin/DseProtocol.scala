/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicLong

import akka.Done
import akka.actor.ActorSystem
import com.datastax.gatling.plugin.metrics.MetricsLogger
import com.datastax.gatling.plugin.request.{CqlRequestActionBuilder, GraphRequestActionBuilder}
import com.datastax.gatling.plugin.utils.GatlingTimingSource
import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.CoreComponents
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.protocol.{Protocol, ProtocolComponents, ProtocolKey}
import io.gatling.core.session.Session

import scala.collection.mutable

/**
  * How things work:
  *
  * - The user calls [[DseProtocolBuilder.session()]] explicitly.
  * This happens when writing the `setUp(scenarios).protocol(...)` code.
  *
  * - The case class method [[DseProtocolBuilder.build]] is called by Gatling.
  * This creates an instance of [[DseProtocol]].
  *
  * - The [[CqlRequestActionBuilder]] or [[GraphRequestActionBuilder]] classes calls Gatling
  * in order to get an instance of [[DseComponents]] from the [[DseProtocol.DseProtocolKey]].
  * This happens for *every scenario*.
  *
  * - The plugin will create one instance of [[DseComponents]] per scenario, as requested,
  * but will reuse the same set of shared components within a given [[akka.actor.ActorSystem]].  This ensures that we
  * do not create several thread pools or HDR Histogram writers, while making it possible to invoke Gatling multiple times
  * in the same JVM in integration tests.
  *
  * - The [[DseComponents]] instance is passed in the plugin code to provide histogram logging features.
  *
  * Multiple [[DseComponents]] may be instantiated, as there is one per scenario.  This can
  * happen, for instance, when there is a warm-up scenario and a real scenario. This is a consequence of Gatling
  * architecture, where each scenario has its own context, and therefore its own component registry.
  */
object DseProtocol extends StrictLogging {
  val DseProtocolKey = new ProtocolKey {
    type Protocol = DseProtocol
    type Components = DseComponents

    def protocolClass: Class[io.gatling.core.protocol.Protocol] =
      classOf[DseProtocol].asInstanceOf[Class[io.gatling.core.protocol.Protocol]]

    def defaultProtocolValue(configuration: GatlingConfiguration): DseProtocol =
      throw new IllegalStateException("Can't provide a default value for DseProtocol")

    def newComponents(system: ActorSystem, coreComponents: CoreComponents): DseProtocol => DseComponents =
      dseProtocol => DseComponents.componentsFor(dseProtocol, system)
  }
}

case class DseProtocol(session: CqlSession) extends Protocol

object DseComponents {
  private val componentsCache = mutable.Map[ActorSystem, DseComponents]()

  def componentsFor(dseProtocol: DseProtocol, system: ActorSystem): DseComponents = synchronized {
    if (componentsCache.contains(system)) {
      // Reuse shared components to avoid creating multiple loggers and executor services in the same simulation
      val shared: DseComponents = componentsCache(system)
      DseComponents(dseProtocol, shared.metricsLogger, shared.dseExecutorService, GatlingTimingSource())
    } else {
      // In integration tests, multiple simulations may be submitted to different Gatling instances of the same JVM
      // Make sure that each actor system gets it own set of shared components
      // This solves the "one set of components per scenario" problem as they are shared across scenarios
      // This also solves the "one set of components per JVM" problem as they are only shared per actor system

      // Create one executor service to handle the plugin taks
      val dseExecutorService = Executors.newFixedThreadPool(
        Runtime.getRuntime.availableProcessors() - 1,
        new ThreadFactory() {
          val identifierGenerator = new AtomicLong()

          override def newThread(r: Runnable): Thread =
            new Thread(r,
              "gatling-dse-plugin-" + identifierGenerator.getAndIncrement())
        })

      // Create a single results recorder per run
      val metricsLogger = MetricsLogger.newMetricsLogger(system, System.currentTimeMillis())

      // Create and cache the shared components for this actor system
      // Register the cleanup task just before the actor system terminates
      val dseComponents = DseComponents(dseProtocol, metricsLogger, dseExecutorService, GatlingTimingSource())
      componentsCache.put(system, dseComponents)
      system.registerOnTermination(dseComponents.shutdown())
      dseComponents
    }
  }
}

case class DseComponents(dseProtocol: DseProtocol,
                         metricsLogger: MetricsLogger,
                         dseExecutorService: ExecutorService,
                         gatlingTimingSource: GatlingTimingSource) extends ProtocolComponents with StrictLogging {
  def onStart: Option[Session => Session] = None

  def onExit: Option[Session => Unit] = None

  def shutdown(): CompletionStage[Done] = {
    logger.info("Shutting down metrics logger")
    metricsLogger.close()
    logger.info("Shutting down thread pool")
    val missed = dseExecutorService.shutdownNow().size()
    if (missed > 0) {
      logger.warn("{} tasks were not completed because of shutdown", missed)
    }
    logger.info("Shut down complete")
    CompletableFuture.completedFuture(Done)
  }
}


object DseProtocolBuilder {
  def session(session: CqlSession) = DseProtocolBuilder(session)
}

case class DseProtocolBuilder(session: CqlSession) {
  def build = DseProtocol(session)
}

