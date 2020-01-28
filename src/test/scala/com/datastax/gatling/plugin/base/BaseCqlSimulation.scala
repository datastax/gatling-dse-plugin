package com.datastax.gatling.plugin.base

import io.gatling.core.scenario.Simulation

abstract class BaseCqlSimulation extends Simulation {

  val testKeyspace = "gatling_cql_unittests"

  val session = GatlingCqlSession.createCqlSession()

  def createTestKeyspace = {
    session.execute(
      s"""CREATE KEYSPACE IF NOT EXISTS $testKeyspace WITH replication = { 'class' : 'SimpleStrategy',
                                          'replication_factor': '1'}""")
  }
}
