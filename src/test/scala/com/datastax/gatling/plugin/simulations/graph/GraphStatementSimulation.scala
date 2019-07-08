package com.datastax.gatling.plugin.simulations.graph

import com.datastax.dse.driver.api.core.graph.{GraphSession, GraphStatement}
import com.datastax.dse.graph.api.DseGraph
import com.datastax.gatling.plugin.GraphPredef._
import com.datastax.gatling.plugin.base.BaseGraphSimulation
import com.datastax.gatling.plugin.model.GraphStringStatement
import io.gatling.core.Predef._
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.scalatest.Ignore
import com.datastax.oss.protocol.internal.ProtocolConstants._

import scala.concurrent.duration.DurationInt

@Ignore
class GraphStatementSimulation extends BaseGraphSimulation {

  val table_name = "test_table"

  session.getConfiguration.getGraphOptions.setGraphName("demo")

  val graphConfig = graph.session(session) //Initialize Gatling DSL with your session

  val r = scala.util.Random

  val graphStatement = new GraphStringStatement("g.addV(label, vertexLabel).property('type', myType)")

  def getInt: String = {
    "test_" + r.nextInt(100).toString
  }

  val insertGraph = graph("Graph Statement")
      .executeGraphStatement(graphStatement)
      .withSetParams(Array("vertexLabel", "myType"))
      .consistencyLevel(ConsistencyLevel.LOCAL_ONE)

  val queryGraph = graph("Graph Query")
      .executeGraph("g.V().limit(5)")

  val g = DseGraph.traversal((GraphSession) session)
  val t: GraphTraversal[_,_] = g.V().limit(5)
  val st: GraphStatement[String] = DseGraph.statementFromTraversal(t)

  val queryGraphNative = graph("Graph Fluent")
      .executeGraphFluent(st)

  val queryGraphFeederTraversal = graph("Graph Feeder")
    .executeGraphFeederTraversal("traversal")

  val feeder = Iterator.continually(
    Map[String, Any](
      "vertexLabel" -> getInt,
      "myType" -> r.nextInt(100),
      "traversal" -> t
    )
  )

  val scn = scenario("PreparedStatement")
      .feed(feeder)
      .exec(insertGraph)

      .pause(1.seconds)
      .exec(queryGraph
          .check(rowCount greaterThan 1)
      )
      .pause(1.seconds)
      .exec(queryGraphNative)
      .pause(1.seconds)
      .exec(queryGraphFeederTraversal)
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
