package com.datastax.gatling.plugin.simulations.cql

import com.datastax.gatling.plugin.DsePredef._
import com.datastax.gatling.plugin.base.BaseCqlSimulation
import com.datastax.oss.driver.api.core.cql.Row
import io.gatling.core.Predef._

import scala.concurrent.duration.DurationInt

class NamedStatementSimulation extends BaseCqlSimulation {

  val table_name = "test_table_named"

  createTestKeyspace
  createTable

  val cqlConfig = cql.session(session) //Initialize Gatling DSL with your session

  val insertId = 2
  val insertStr = "two"

  val statementInsert = s"""INSERT INTO $testKeyspace.$table_name (id, str) VALUES (:id, :str)"""
  val statementSelect = s"""SELECT * FROM $testKeyspace.$table_name WHERE id = :id"""

  val preparedInsert = session.prepare(statementInsert)
  val preparedSelect = session.prepare(statementSelect)


  val feeder = Iterator.continually(
    // this feader will "feed" random data into our Sessions
    Map(
      "id" -> insertId,
      "str" -> insertStr
    )
  )

  val insertCql = cql("NamedParam Insert Statement")
      .executeNamed(preparedInsert)

  val selectCql = cql("NamedParam Select Statement")
      .executeNamed(preparedSelect)

  private def selectCqlExtract(row:Row):String =
    row.getString("str")

  val scn = scenario("NamedStatement")
      .feed(feeder)
      .exec(insertCql
          .check(resultSet.transform(_.hasMorePages) is false)
          .check(resultSet.transform(_.remaining) is 0) // "normal" INSERTs don't return anything
      )
      .pause(1.seconds)

      .exec(selectCql
          .check(resultSet.transform(_.remaining) is 1)
          .check(resultSet.transform(rs => selectCqlExtract(rs.one)) is insertStr)
      )


  setUp(
    scn.inject(
      constantUsersPerSec(1) during 1.seconds)
  ).assertions(
    global.failedRequests.count.is(0)
  ).protocols(cqlConfig)


  def createTable = {
    val table =
      s"""
      CREATE TABLE IF NOT EXISTS $testKeyspace.$table_name (
      id int,
      str text,
      PRIMARY KEY (id)
    );"""

    session.execute(table)

    //session.execute(s"""TRUNCATE TABLE $testKeyspace.$table_name""")
  }
}
