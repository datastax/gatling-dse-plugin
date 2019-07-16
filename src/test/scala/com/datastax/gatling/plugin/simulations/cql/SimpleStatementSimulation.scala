package com.datastax.gatling.plugin.simulations.cql

import com.datastax.gatling.plugin.CqlPredef._
import com.datastax.gatling.plugin.base.BaseCqlSimulation
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

  val scn = scenario("SimpleStatement")
      .exec(insertCql
          .check(exhausted is true)
          .check(rowCount is 0) // "normal" INSERTs don't return anything
      )
      .pause(1.seconds)
      .group("TestGroup") {
        exec(selectCql
            .check(rowCount is 1)
            .check(columnValue("str") is insertStr)
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
