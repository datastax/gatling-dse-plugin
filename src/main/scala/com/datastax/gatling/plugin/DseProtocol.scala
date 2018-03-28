package com.datastax.gatling.plugin

import java.util.concurrent.{CompletableFuture, Executors, ScheduledExecutorService}

import akka.Done
import akka.actor.{ActorSystem, CoordinatedShutdown}
import com.datastax.driver.dse.DseSession
import com.datastax.gatling.plugin.metrics.HistogramLogger
import io.gatling.core.CoreComponents
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.protocol.{Protocol, ProtocolComponents, ProtocolKey}
import io.gatling.core.session.Session

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
  * - Gatling instantiate a new instance of [[com.datastax.gatling.plugin.DseComponents]] by calling
  * [[com.datastax.gatling.plugin.DseProtocol.DseProtocolKey.newComponents()]]
  * It stores it for future use.
  *
  * - The [[com.datastax.gatling.plugin.DseComponents]] instance is passed in the plugin code to provide histogram
  * logging features.
  *
  * Multiple [[com.datastax.gatling.plugin.DseComponents]] may be instantiated, as there is one per scenario.  This can
  * happen, for instance, when there is a warm-up scenario and a real scenario. This is a consequence of Gatling
  * architecture, where each scenario has its own context, and therefore its own component registry.
  *
  * However, every [[com.datastax.gatling.plugin.metrics.HistogramLogger]] use the same output directory.  Therefore,
  * no action should have the same name, otherwise HDR Histogram files would contain results of different scenarios.
  */
object DseProtocol {
  private val startTime: Long = System.currentTimeMillis()
  val DseProtocolKey = new ProtocolKey {

    type Protocol = DseProtocol
    type Components = DseComponents

    def protocolClass: Class[io.gatling.core.protocol.Protocol] =
      classOf[DseProtocol].asInstanceOf[Class[io.gatling.core.protocol.Protocol]]

    def defaultProtocolValue(configuration: GatlingConfiguration): DseProtocol =
      throw new IllegalStateException("Can't provide a default value for DseProtocol")

    def newComponents(system: ActorSystem, coreComponents: CoreComponents): DseProtocol => DseComponents = {
      dseProtocol => {
        // The histogram logger need to be explicitly closed for global histograms to be written
        // Previously, clients had to include an `after { HistogramLogger.close() }` call in every simulation
        // This takes care of shutting down the logger after Akka has shut down all the other actors
        val histogramLogger = new HistogramLogger(system, startTime)
        CoordinatedShutdown(system).addTask(
          CoordinatedShutdown.PhaseBeforeActorSystemTerminate,
          "Write remaining histograms",
          () => CompletableFuture.completedFuture {
            CompletableFuture.completedFuture(histogramLogger.close())
            Done
          }
        )

        // 2017-12-05 Some queries timeouts may be lost for an obscure reason, create a thread to force a timeout
        // Make sure that this thread is stopped before Gatling finishes so that it does not prevent the JVM shutdown
        val timeoutExecutor = Executors.newSingleThreadScheduledExecutor()
        CoordinatedShutdown(system).addTask(
          CoordinatedShutdown.PhaseBeforeActorSystemTerminate,
          "Shut down timeout enforcer",
          () => CompletableFuture.completedFuture {
            timeoutExecutor.shutdownNow()
            Done
          }
        )

        DseComponents(dseProtocol, histogramLogger, timeoutExecutor)
      }
    }
  }
}

//holds reference to a cluster, just settings
case class DseProtocol(session: DseSession) extends Protocol

case class DseComponents(dseProtocol: DseProtocol,
                         histogramLogger: HistogramLogger,
                         timeoutExecutor: ScheduledExecutorService) extends ProtocolComponents {

  def onStart: Option[Session => Session] = None

  def onExit: Option[Session => Unit] = None
}


object DseProtocolBuilder {
  def session(session: DseSession) = DseProtocolBuilder(session)
}

case class DseProtocolBuilder(session: DseSession) {
  def build = DseProtocol(session)
}

