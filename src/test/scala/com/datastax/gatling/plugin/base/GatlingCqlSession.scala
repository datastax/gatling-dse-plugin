package com.datastax.gatling.plugin.base

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicReference

import com.datastax.oss.driver.api.core.CqlSession
import org.cassandraunit.utils.EmbeddedCassandraServerHelper

trait GatlingCqlSession {

  // TODO: Probably should eventually be an actor, using AtomicRef for now to cheat
  private val session: AtomicReference[CqlSession] = new AtomicReference[CqlSession](null)

  /**
    * Create new Dse Session to either the embedded C* instance or a remote instance
    *
    * Note: This includes a hack to get around issue with DseCluster builder and Scala
    *
    * @param cassandraHost Cassandra Server IP
    * @param cassandraPort Cassandra Port, default will use Embedded Cassandra's port
    * @return
    */
  def createCqlSession(cassandraHost: String = "127.0.0.1",
                       cassandraPort: Int = EmbeddedCassandraServerHelper.getNativeTransportPort,
                       localDc:String = "datacenter1"): CqlSession = {

    session.updateAndGet((v) => {
      if (v != null) {
        v
      }
      else {
        val addr = new InetSocketAddress(cassandraHost, cassandraPort)
        CqlSession.builder().addContactPoint(addr).withLocalDatacenter(localDc).build()
      }
    })
  }


  /**
    * Get the session of the current DSE Session
    *
    * @return
    */
  def getSession: CqlSession = {
    createCqlSession()
  }


  /**
    * Close the current session
    */
  def closeSession(): Unit = {
    session.getAndSet(null).close()
  }

}

object GatlingCqlSession extends GatlingCqlSession
