package com.datastax.gatling.plugin.simulations.cql

import com.datastax.gatling.plugin.DsePredef._
import com.datastax.gatling.plugin.base.BaseCqlSimulation
import com.datastax.oss.driver.api.core.cql.Row
import io.gatling.core.Predef._

import scala.concurrent.duration.DurationInt

class SimpleStatementSimulation extends BaseCqlSimulation {

  val table_name = "test_table_simple"

  createTestKeyspace
  createTable

  val dseProtocol = dseProtocolBuilder.session(session) //Initialize Gatling DSL with your session

  val insertId = 1
  val insertStr = "one"

  val simpleStatementInsert = s"""INSERT INTO $testKeyspace.$table_name (id, str) VALUES ($insertId, '$insertStr')"""
  val simpleStatementSelect = s"""SELECT * FROM $testKeyspace.$table_name WHERE id = $insertId"""

  val insertCql = cql("Insert_Statement")
      .executeCql(simpleStatementInsert)

  val selectCql = cql("Select_Statement")
      .executeCql(simpleStatementSelect)

  private def selectCqlExtract(row:Row):String =
    row.getString("str")

  val scn = scenario("SimpleStatement")
      .exec(insertCql
          .check(resultSet.transform(_.hasMorePages) is false)
          .check(resultSet.transform(_.remaining) is 0) // "normal" INSERTs don't return anything
      )
      .pause(1.seconds)
      .group("TestGroup") {
        exec(selectCql
            .check(resultSet.transform(_.remaining) is 1)
            .check(resultSet.transform(rs => selectCqlExtract(rs.one)) is insertStr)
        )
      }

  setUp(
    scn.inject(constantUsersPerSec(150) during 1.seconds).protocols(dseProtocol)
  ).assertions(
    global.failedRequests.count.is(0)
  )

  def createTable = {
    val table =
      s"""
      CREATE TABLE IF NOT EXISTS $testKeyspace.$table_name (
      id int,
      str text,
      PRIMARY KEY (id)
    );"""

    session.execute(table)
  }
}
