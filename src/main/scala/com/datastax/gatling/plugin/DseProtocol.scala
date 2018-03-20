package com.datastax.gatling.plugin

import java.util.concurrent.{CompletableFuture, CompletionStage, Executors, ScheduledExecutorService}

import akka.Done
import akka.actor.{ActorRef, ActorSystem, CoordinatedShutdown, Props}
import akka.routing.RoundRobinPool
import com.datastax.driver.dse.DseSession
import com.datastax.gatling.plugin.metrics.MetricsLogger
import com.datastax.gatling.plugin.request.DseRequestActor
import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.CoreComponents
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.protocol.{Protocol, ProtocolComponents, ProtocolKey}
import io.gatling.core.session.Session

import scala.collection.mutable

/**
  * How things work:
  *
  * - The user calls [[com.datastax.gatling.plugin.DseProtocolBuilder.session()]] explicitly.
  * This happens when writing the `setUp(scenarios).protocol(...)` code.
  *
  * - The case class method [[com.datastax.gatling.plugin.DseProtocolBuilder.build]] is called by Gatling.
  * This creates an instance of [[com.datastax.gatling.plugin.DseProtocol]].
  *
  * - The [[com.datastax.gatling.plugin.request.DseRequestActionBuilder]] class calls Gatling in order to get an
  * instance of [[com.datastax.gatling.plugin.DseComponents]] from the [[com.datastax.gatling.plugin.DseProtocol.DseProtocolKey]].
  * This happens for *every scenario*.
  *
  * - The plugin will create one instance of [[com.datastax.gatling.plugin.DseComponents]] per scenario, as requested,
  * but will reuse the same set of shared components within a given [[akka.actor.ActorSystem]].  This ensures that we
  * do not create several routers or HDR Histogram writers, while making it possible to invoke Gatling multiple times
  * in the same JVM in integration tests.
  *
  * - The [[com.datastax.gatling.plugin.DseComponents]] instance is passed in the plugin code to provide histogram
  * logging features.
  *
  * Multiple [[com.datastax.gatling.plugin.DseComponents]] may be instantiated, as there is one per scenario.  This can
  * happen, for instance, when there is a warm-up scenario and a real scenario. This is a consequence of Gatling
  * architecture, where each scenario has its own context, and therefore its own component registry.
  */
object DseProtocol {
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

case class DseProtocol(session: DseSession) extends Protocol

object DseComponents {
  private val componentsCache = mutable.Map[ActorSystem, DseComponents]()

  def componentsFor(dseProtocol: DseProtocol, system: ActorSystem): DseComponents = synchronized {
    if (componentsCache.contains(system)) {
      // Reuse shared components to avoid creating multiple loggers and routers in the same simulation
      val shared: DseComponents = componentsCache(system)
      DseComponents(dseProtocol, shared.metricsLogger, shared.dseRequestsRouter)
    } else {
      // In integration tests, multiple simulations may be submitted to different Gatling instances of the same JVM
      // Make sure that each actor system gets it own set of shared components
      // This solves the "one set of components per scenario" problem as they are shared across scenarios
      // This also solves the "one set of components per JVM" problem as they are only shared per actor system

      // Create one router with several actors to handle the CQL work
      val router = system.actorOf(
        RoundRobinPool(Runtime.getRuntime.availableProcessors())
          .props(Props[DseRequestActor]), "dse-requests-router")

      // The histogram logger need to be explicitly closed for global histograms to be written
      // Previously, clients had to include an `after { HistogramLogger.close() }` call in every simulation
      // This takes care of shutting down the logger after Akka has shut down all the other actors
      val metricsLogger = MetricsLogger.newMetricsLogger(system, System.currentTimeMillis())

      // Create and cache the shared components for this actor system
      // Register the cleanup task just before the actor system terminates
      val dseComponents = DseComponents(dseProtocol, metricsLogger, router)
      componentsCache.put(system, dseComponents)
      CoordinatedShutdown(system).addTask(
        CoordinatedShutdown.PhaseBeforeActorSystemTerminate,
        "Shut down shared components",
        () => dseComponents.shutdown()
      )
      dseComponents
    }
  }
}

case class DseComponents(dseProtocol: DseProtocol,
                         metricsLogger: MetricsLogger,
                         dseRequestsRouter: ActorRef) extends ProtocolComponents with StrictLogging {
  def onStart: Option[Session => Session] = None

  def onExit: Option[Session => Unit] = None

  def shutdown(): CompletionStage[Done] = {
    logger.info("Shutting down metrics logger")
    metricsLogger.close()
    logger.info("Shut down complete")
    CompletableFuture.completedFuture(Done)
  }
}


object DseProtocolBuilder {
  def session(session: DseSession) = DseProtocolBuilder(session)
}

case class DseProtocolBuilder(session: DseSession) {
  def build = DseProtocol(session)
}

