package com.datastax.gatling.plugin.simulations

import com.datastax.gatling.plugin.base.BaseSimulationSpec
import io.gatling.app.Gatling
import org.scalatest.Ignore

@Ignore
class GraphSimulationsSpec extends BaseSimulationSpec {

  describe("Graph_SimpleGraphStatement_Simulation") {

    ignore("should succeed with 0 failures") {
      val props = getGatlingProps.simulationClass(graphSimulationDir + "GraphStatementSimulation")
      Gatling.fromMap(props.build) shouldBe 0
    }
  }

}
