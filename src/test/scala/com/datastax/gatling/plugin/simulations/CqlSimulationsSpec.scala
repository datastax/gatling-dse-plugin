package com.datastax.gatling.plugin.simulations

import com.datastax.gatling.plugin.base.BaseSimulationSpec
import io.gatling.app.Gatling


class CqlSimulationsSpec extends BaseSimulationSpec {

  describe("Cql_SimpleStatement_Simulation") {
    it("should succeed with 0 failures") {
      val props = getGatlingProps.simulationClass(cqlSimulationDir + "SimpleStatementSimulation")
      Gatling.fromMap(props.build) shouldBe 0
    }
  }

  describe("Cql_PreparedStatement_Simulation") {
    it("should succeed with 0 failures") {
      val props = getGatlingProps.simulationClass(cqlSimulationDir + "PreparedStatementSimulation")
      Gatling.fromMap(props.build) shouldBe 0
    }
  }

  describe("Cql_NamedStatement_Simulation") {
    it("should succeed with 0 failures") {
      val props = getGatlingProps.simulationClass(cqlSimulationDir + "NamedStatementSimulation")
      Gatling.fromMap(props.build) shouldBe 0
    }
  }

  describe("Cql_Udt_Simulation") {
    it("should succeed with 0 failures") {
      val props = getGatlingProps.simulationClass(cqlSimulationDir + "UdtStatementSimulation")
      Gatling.fromMap(props.build) shouldBe 0
    }
  }

  describe("Bound_CqlTypes_Simulation") {
    it("should succeed with 0 failures") {
      val props = getGatlingProps.simulationClass(cqlSimulationDir + "BoundCqlTypesSimulation")
      Gatling.fromMap(props.build) shouldBe 0
    }
  }

  describe("Batch_Statement_Simulation") {
    it("should succeed with 0 failures") {
      val props = getGatlingProps.simulationClass(cqlSimulationDir + "BatchStatementSimulation")
      Gatling.fromMap(props.build) shouldBe 0
    }
  }

}
