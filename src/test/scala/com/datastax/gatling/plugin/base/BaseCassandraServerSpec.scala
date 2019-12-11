package com.datastax.gatling.plugin.base

import java.nio.file.Files

import com.datastax.dse.driver.api.core.DseSession
import org.cassandraunit.utils.EmbeddedCassandraServerHelper

/**
  * Used for Specs that require a running Cassandra instance to run
  */
class BaseCassandraServerSpec extends BaseSpec {
  protected val dseSession: DseSession = BaseCassandraServerSpec.dseSession

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

object BaseCassandraServerSpec {
  if (EmbeddedCassandraServerHelper.getSession == null) {
    EmbeddedCassandraServerHelper.startEmbeddedCassandra(
      "cassandra.yaml",
      Files.createTempDirectory("gatling-dse-plugin.").toString,
      30000L)
  }

  private val dseSession: DseSession = GatlingDseSession.getSession
}
