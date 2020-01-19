package com.datastax.gatling.plugin.simulations.cql

import com.datastax.gatling.plugin.DsePredef._
import com.datastax.gatling.plugin.base.BaseCqlSimulation
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
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

  val statementInsert = QueryBuilder.insertInto(testKeyspace, table_name)
    .value("id", QueryBuilder.bindMarker())
    .value("str", QueryBuilder.bindMarker())
    .value("name", QueryBuilder.bindMarker())
    .build()
  val statementSelect = QueryBuilder.selectFrom(testKeyspace, table_name)
    .all()
    .whereColumn("id").isEqualTo(QueryBuilder.bindMarker())
    .whereColumn("str").isEqualTo(QueryBuilder.bindMarker())
    .build()

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
      .executeStatement(preparedInsert)
      .withParams("${id}", "${str}", "${name}")

  val selectCql = cql("Select_Statement")
      .executeStatement(preparedSelect)
      .withParams("${id}", "${str}")

  val selectCqlSessionParam = cql("Select_Statement_Array")
      .executeStatement(preparedSelect)
      .withParams(List("id", "str"))

  private def selectCqlExtract(row:Row):String =
    row.getString("name")

  val scnPassed = scenario("ABCPreparedStatement")
      .feed(feeder)
      .exec(insertCql
          .check(resultSet.transform(_.hasMorePages) is false)
          .check(resultSet.transform(_.remaining) is 0) // "normal" INSERTs don't return anything
      )
      .pause(1.seconds)

      .exec(selectCql
          .check(resultSet.transform(_.remaining) is 1)
          .check(resultSet.transform(rs => selectCqlExtract(rs.one)) is insertName)
      )
      .pause(1.seconds)

      .exec(selectCqlSessionParam
          .check(resultSet.transform(_.remaining) is 1)
          .check(resultSet.transform(rs => selectCqlExtract(rs.one)) is insertName)
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
