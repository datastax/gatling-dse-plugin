package com.datastax.gatling.plugin.base

import java.net.InetSocketAddress

import com.datastax.oss.driver.api.core.CqlSession
import org.cassandraunit.utils.EmbeddedCassandraServerHelper

trait GatlingCqlSession {

  private var session: CqlSession = _

  /**
    * Create new Dse Session to either the embedded C* instance or a remote instance
    *
    * Note: This includes a hack to get around issue with DseCluster builder and Scala
    *
    * @param cassandraHost Cassandra Server IP
    * @param cassandraPort Cassandra Port, default will use Embedded Cassandra's port
    * @return
    */
  def createCqlSession(cassandraHost: String = "127.0.0.1", cassandraPort: Int = -1): CqlSession = {

    if (session != null) {
      return session
    }

    var cPort = cassandraPort
    if (cPort == -1) {
      cPort = EmbeddedCassandraServerHelper.getNativeTransportPort
    }

    session =
        try {
          CqlSession.builder().addContactPoint(new InetSocketAddress(cassandraHost, cPort)).withLocalDatacenter("datacenter1").build()
        }
        catch {
          case _: Exception => CqlSession.builder().addContactPoint(new InetSocketAddress(cassandraHost, cPort)).build()
        }

    session
  }


  /**
    * Get the session of the current DSE Session
    *
    * @return
    */
  def getSession: CqlSession = {
    if (session == null) {
      createCqlSession()
    }
    session
  }


  /**
    * Close the current session
    */
  def closeSession(): Unit = {
    session.close()
  }

}

object GatlingCqlSession extends GatlingCqlSession
