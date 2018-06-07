package com.datastax.gatling.plugin.simulations

import com.datastax.gatling.plugin.base.BaseSimulationSpec
import io.gatling.app.Gatling
import org.scalatest.Ignore

class GraphSimulationsSpec extends BaseSimulationSpec {

  describe("Graph_SimpleGraphStatement_Simulation") {

    it("should succeed with 0 failures") {
      val props = getGatlingProps.simulationClass(graphSimulationDir + "GraphStatementSimulation")
      // embedded cassandra doesn't support graph queries so we expect gatling to fail but it
      // should complete properly.
      Gatling.fromMap(props.build) shouldBe 2
    }
  }

}
