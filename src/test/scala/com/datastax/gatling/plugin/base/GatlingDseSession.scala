package com.datastax.gatling.plugin.base

import java.net.InetSocketAddress

import com.datastax.dse.driver.api.core.DseSession
import org.cassandraunit.utils.EmbeddedCassandraServerHelper

trait GatlingDseSession {

  private var session: DseSession = _

  /**
    * Create new Dse Session to either the embedded C* instance or a remote instance
    *
    * Note: This includes a hack to get around issue with DseCluster builder and Scala
    *
    * @param cassandraHost Cassandra Server IP
    * @param cassandraPort Cassandra Port, default will use Embedded Cassandra's port
    * @return
    */
  def createDseSession(cassandraHost: String = "127.0.0.1", cassandraPort: Int = -1): DseSession = {

    if (session != null) {
      return session
    }

    var cPort = cassandraPort
    if (cPort == -1) {
      cPort = EmbeddedCassandraServerHelper.getNativeTransportPort
    }

    session =
        try {
          DseSession.builder().addContactPoint(new InetSocketAddress(cassandraHost, cPort)).build()
        }
        catch {
          case _: Exception => DseSession.builder().addContactPoint(new InetSocketAddress(cassandraHost, cPort)).build()
        }

    session
  }


  /**
    * Get the session of the current DSE Session
    *
    * @return
    */
  def getSession: DseSession = {
    if (session == null) {
      createDseSession()
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

object GatlingDseSession extends GatlingDseSession
