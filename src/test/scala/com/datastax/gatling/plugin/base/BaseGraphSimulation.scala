package com.datastax.gatling.plugin.base

import io.gatling.core.scenario.Simulation

abstract class BaseGraphSimulation extends Simulation {

  val testKeyspace = "gatling_cql_unittests"

  val session = GatlingCqlSession.createCqlSession("10.10.10.2", 9042)

  def createTestKeyspace = {
    session.execute(
      s"""CREATE KEYSPACE IF NOT EXISTS $testKeyspace WITH replication = { 'class' : 'SimpleStrategy',
                                          'replication_factor': '1'}""")
  }

}
