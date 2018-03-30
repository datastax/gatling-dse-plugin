package com.datastax.gatling.plugin.simulations.cql

import com.datastax.gatling.plugin.DsePredef._
import com.datastax.gatling.plugin.base.BaseCqlSimulation
import io.gatling.core.Predef._

import scala.concurrent.duration.DurationInt

class PreparedStatementSimulation extends BaseCqlSimulation {

  val table_name = "test_table_prepared"

  createTestKeyspace
  createTable

  val cqlConfig = cql.session(session) //Initialize Gatling DSL with your session

  val insertId = 2
  val insertStr = "two"
  val insertName = "test"

  val statementInsert = s"""INSERT INTO $testKeyspace.$table_name (id, str, name) VALUES (?, ?, ?)"""
  val statementSelect = s"""SELECT * FROM $testKeyspace.$table_name WHERE id = ? AND str = ?"""

  val preparedInsert = session.prepare(statementInsert)
  val preparedSelect = session.prepare(statementSelect)


  // this feeder will "feed" random data into our Sessions
  val feeder = Iterator.continually(
    Map(
      "id" -> insertId,
      "str" -> insertStr,
      "name" -> insertName
    )
  )

  val insertCql = cql("Insert_Statement")
      .executePrepared(preparedInsert)
      .withParams("${id}", "${str}", "${name}")

  val selectCql = cql("Select_Statement")
      .executePrepared(preparedSelect)
      .withParams("${id}", "${str}")

  val selectCqlSessionParam = cql("Select_Statement_Array")
      .executePrepared(preparedSelect)
      .withParams(List("id", "str"))


  val scnPassed = scenario("ABCPreparedStatement")
      .feed(feeder)
      .exec(insertCql
          .check(exhausted is true)
          .check(rowCount is 0) // "normal" INSERTs don't return anything
      )
      .pause(1.seconds)

      .exec(selectCql
          .check(rowCount is 1)
          .check(columnValue("name") is insertName)
      )
      .pause(1.seconds)

      .exec(selectCqlSessionParam
          .check(rowCount is 1)
          .check(columnValue("name") is insertName)
      )


  setUp(
    scnPassed.inject(
      constantUsersPerSec(1) during 1.seconds
    )
  ).assertions(
    global.failedRequests.count.is(0)
  ).protocols(cqlConfig)


  def createTable = {
    val table =
      s"""
      CREATE TABLE IF NOT EXISTS $testKeyspace.$table_name (
      id int,
      str text,
      name text,
      PRIMARY KEY (id, str)
    );"""

    session.execute(table)
  }
}
