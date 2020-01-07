package com.datastax.gatling.plugin.simulations.graph

import com.datastax.dse.driver.api.core.graph.{DseGraph, FluentGraphStatement, ScriptGraphStatement}
import com.datastax.gatling.plugin.DsePredef._
import com.datastax.gatling.plugin.base.BaseGraphSimulation
import com.datastax.oss.driver.api.core.ConsistencyLevel
import io.gatling.core.Predef._
import org.scalatest.Ignore

import scala.concurrent.duration.DurationInt

@Ignore
class GraphStatementSimulation extends BaseGraphSimulation {

  val table_name = "test_table"
  val graph_name = "demo"

  val graphConfig = graph.session(session) //Initialize Gatling DSL with your session

  val r = scala.util.Random

  def getInt: String = {
    "test_" + r.nextInt(100).toString
  }

  val insertStatement = ScriptGraphStatement.newInstance("g.addV(label, vertexLabel).property('type', myType)")
  val insertGraph = graph("Graph Statement")
      .executeGraph(insertStatement)
      .withParams("vertexLabel", "myType")
      .withConsistencyLevel(ConsistencyLevel.LOCAL_ONE)
      .withName(graph_name)

  val queryGraph = graph("Graph Query")
      .executeGraph("g.V().limit(5)")
      .withName(graph_name)

  val queryStatement: FluentGraphStatement = FluentGraphStatement.newInstance(DseGraph.g.V().limit(5))
  val queryGraphNative = graph("Graph Fluent")
      .executeGraphFluent(queryStatement)
      .withName(graph_name)

  val feeder = Iterator.continually(
    Map[String, Any](
      "vertexLabel" -> getInt,
      "myType" -> r.nextInt(100)
    )
  )

  val scn = scenario("PreparedStatement")
      .feed(feeder)
      .exec(insertGraph)

      .pause(1.seconds)
      .exec(queryGraph
          .check(graphResultSet.transform(_.remaining) greaterThan 1)
      )
      .pause(1.seconds)
      .exec(queryGraphNative)
      .exec(session => {
        //    println(session("test").asOption[String].toString)
        session
      })


  setUp(
    scn.inject(
      constantUsersPerSec(1) during 10.second)
  ).assertions(
    global.failedRequests.count.is(0)
  ).protocols(graphConfig)

}
