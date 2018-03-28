package com.datastax.gatling.plugin.base

import com.datastax.driver.dse.DseSession
import org.cassandraunit.utils.EmbeddedCassandraServerHelper

/**
  * Used for Specs that require a running Cassandra instance to run
  */
class BaseCassandraServerSpec extends BaseSpec {

  private val cassandraBaseDir = "./build/cassandraUnit/"

  // Create Embedded Cassandra Server to run queries against
  if (EmbeddedCassandraServerHelper.getSession == null) {
    //    println("Starting embedded Cassandra instance...")
    EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra.yaml", cassandraBaseDir, 30000L)
  }

  protected val dseSession: DseSession = GatlingDseSession.getSession


  protected def cleanCassandra(keyspace: String = ""): Unit = {
    if (keyspace.isEmpty) {
      EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
    } else {
      EmbeddedCassandraServerHelper.cleanDataEmbeddedCassandra(keyspace)
    }
  }

  protected def createKeyspace(keyspace: String): Boolean = {
    dseSession.execute(
      s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication = " +
          "{ 'class' : 'SimpleStrategy', 'replication_factor': '1'}")
        .wasApplied()
  }

  protected def createTable(keyspace: String, name: String, columns: String): Boolean = {
    dseSession.execute(
      s"""
        CREATE TABLE IF NOT EXISTS $keyspace.$name (
        $columns
      );""").wasApplied()
  }


  protected def createType(keyspace: String, name: String, columns: String): Boolean = {
    dseSession.execute(
      s"""
        CREATE TYPE IF NOT EXISTS $keyspace.$name (
        $columns
      );""").wasApplied()
  }


}
