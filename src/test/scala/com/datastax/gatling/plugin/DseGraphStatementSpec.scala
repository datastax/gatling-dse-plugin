package com.datastax.gatling.plugin

import com.datastax.dse.driver.api.core.DseSession
import com.datastax.dse.driver.api.core.graph.{DseGraph, FluentGraphStatement, ScriptGraphStatement}
import com.datastax.gatling.plugin.base.BaseSpec
import com.datastax.gatling.plugin.model.{GraphBoundStatement, GraphFluentStatement, GraphStringStatement}
import io.gatling.commons.validation.{Failure, Success}
import io.gatling.core.session.Session
import io.gatling.core.session.el.ElCompiler
import org.easymock.EasyMock.reset


class DseGraphStatementSpec extends BaseSpec {

  val mockDseSession = mock[DseSession]
  val validGatlingSession = new Session("name", 1, Map("test" -> "5"))
  val invalidGatlingSession = new Session("name", 1, Map("buzz" -> Map("test" -> "this")))

  before {
    reset(mockDseSession)
  }


  describe("StringStatement") {

    val el = ElCompiler.compile[String]("g.addV(label, vertexLabel).property('type', ${test})")
    val target = GraphStringStatement(el)

    it("should succeed for a valid expression") {
      val result = target.buildFromSession(validGatlingSession)
      result shouldBe a[Success[_]]
    }

    it("should fail if the expression is wrong") {
      val result = target.buildFromSession(invalidGatlingSession)
      result shouldBe a[Failure]
      result shouldBe Failure("No attribute named 'test' is defined")
    }

  }

  describe("FluentStatement") {

    val g = DseGraph.g
    val gStatement = FluentGraphStatement.newInstance(g.V().limit(5))
    val target = GraphFluentStatement(gStatement)

    it("should correctly return StringStatement for a valid expression") {
      val result = target.buildFromSession(validGatlingSession)
      result shouldBe a[Success[_]]
    }
  }

  describe("GraphBoundStatement") {

    val target = GraphBoundStatement(ScriptGraphStatement.newInstance(
      "g.addV(label, vertexLabel).property('type', myType)"), Map("test" -> "type"))

    it("should suceeed with a valid session") {
      val result = target.buildFromSession(validGatlingSession)
      result shouldBe a[Success[_]]
    }

    it("should faile with an invalid session") {
      val result = target.buildFromSession(invalidGatlingSession)
      result shouldBe a[Failure]
    }

  }

}
