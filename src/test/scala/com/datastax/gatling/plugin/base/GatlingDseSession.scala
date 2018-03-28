package com.datastax.gatling.plugin.base

import com.datastax.driver.dse.{DseCluster, DseSession}
import org.cassandraunit.utils.EmbeddedCassandraServerHelper

trait GatlingDseSession {

  private var dseCluster: DseCluster = _

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

    dseCluster =
        try {
          DseCluster.builder().addContactPoint(cassandraHost).withPort(cPort).build()
        }
        catch {
          case _: Exception => DseCluster.builder().addContactPoint(cassandraHost).withPort(cPort).build()
        }

    session = dseCluster.connect()
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
