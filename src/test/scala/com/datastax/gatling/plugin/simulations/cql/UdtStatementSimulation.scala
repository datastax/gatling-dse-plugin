package com.datastax.gatling.plugin.simulations.cql

import com.datastax.gatling.plugin.DsePredef._
import com.datastax.gatling.plugin.base.BaseCqlSimulation
import com.datastax.oss.driver.api.core.`type`.UserDefinedType
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.querybuilder.QueryBuilder
import io.gatling.core.Predef._

import scala.concurrent.duration.DurationInt

class UdtStatementSimulation extends BaseCqlSimulation {

  val table_name = "test_table_udt"
  val udt_name = "fullname"

  createTestKeyspace
  createTable

  val cqlConfig = cql.session(session)
  //Initialize Gatling DSL with your session
  val addressType:UserDefinedType = session.getMetadata.getKeyspace(testKeyspace).flatMap(_.getUserDefinedType("fullname")).get

  val simpleId = 1
  val preparedId = 2
  val namedId = 3
  val insertFullName = addressType.newValue()
      .setString("firstname", "John")
      .setString("lastname", "Smith")

  val simpleStatementInsert = QueryBuilder.insertInto(testKeyspace, table_name)
    .value("id", QueryBuilder.literal(simpleId))
    .value("name", QueryBuilder.literal(insertFullName))
    .build()
  val simpleStatementSelect = QueryBuilder.selectFrom(testKeyspace, table_name)
    .all()
    .whereColumn("id").isEqualTo(QueryBuilder.literal(simpleId))
    .build()

  val insertCql = cql("Insert Simple Statements")
      .executeStatement(simpleStatementInsert)

  val selectCql = cql("Select Simple Statement")
      .executeStatement(simpleStatementSelect)

  val preparedStatementInsert = s"""INSERT INTO $testKeyspace.$table_name (id, name) VALUES (?, ?)"""
  val preparedStatementSelect = s"""SELECT * FROM $testKeyspace.$table_name WHERE id = ?"""

  val preparedInsert = session.prepare(preparedStatementInsert)
  val preparedSelect = session.prepare(preparedStatementSelect)

  val insertPreparedCql = cql("Insert Prepared Statements")
      .executePrepared(preparedInsert)

  val selectPreparedCql = cql("Select Prepared Statement")
      .executePrepared(preparedSelect)

  val preparedFeed = Iterator.continually(
    Map(
      "id" -> preparedId,
      "fullname" -> insertFullName
    )
  )

  val namedStatementInsert = s"""INSERT INTO $testKeyspace.$table_name (id, name) VALUES (:id, :fullname)"""
  val namedStatementSelect = s"""SELECT * FROM $testKeyspace.$table_name WHERE id = :id"""

  val namedInsert = session.prepare(namedStatementInsert)
  val namedSelect = session.prepare(namedStatementSelect)

  val insertNamedCql = cql("Insert Named Statements")
      .executeNamed(namedInsert)

  val selectNamedCql = cql("Select Named Statement")
      .executeNamed(namedSelect)

  val namedFeed = Iterator.continually(
    Map(
      "id" -> namedId,
      "fullname" -> insertFullName
    )
  )

  private def extractFirstNameFromRow(row:Row):String =
    row.getUdtValue("name").getString(0)

  val scn = scenario("SimpleStatement")
      .exec(insertCql
          .check(resultSet.transform(_.hasMorePages) is false)
          .check(resultSet.transform(_.remaining) is 0) // "normal" INSERTs don't return anything
      )
      .pause(100.millis)

      .exec(selectCql
          .check(resultSet.transform(_.remaining) is 1)
          .check(resultSet.transform(rs => extractFirstNameFromRow(rs.one)) not "")
      )
      .pause(100.millis)

      .feed(preparedFeed)
      .exec(insertPreparedCql
          .withParams(List("id", "fullname"))
          .check(resultSet.transform(_.hasMorePages) is false)
          .check(resultSet.transform(_.remaining) is 0) // "normal" INSERTs don't return anything
      )
      .pause(100.millis)

      .exec(selectPreparedCql
          .withParams(List("id"))
          .check(resultSet.transform(_.remaining) is 1)
          .check(resultSet.transform(rs => extractFirstNameFromRow(rs.one)) not "")
      )
      .pause(100.millis)

      .feed(namedFeed)
      .exec(insertNamedCql
          .check(resultSet.transform(_.hasMorePages) is false)
          .check(resultSet.transform(_.remaining) is 0) // "normal" INSERTs don't return anything
      )
      .pause(100.millis)

      .exec(selectNamedCql
          .check(resultSet.transform(_.remaining) is 1)
          .check(resultSet.transform(rs => extractFirstNameFromRow(rs.one)) not "")
      )

  setUp(
    scn.inject(constantUsersPerSec(1) during 1.seconds).protocols(cqlConfig)
  ).assertions(
    global.failedRequests.count.is(0)
  )

  private def createTable = {

    val udt =
      s"""
         |CREATE TYPE IF NOT EXISTS $testKeyspace.fullname (
         |  firstname text,
         |  lastname text
         |);
       """.stripMargin

    val table =
      s"""
         |CREATE TABLE IF NOT EXISTS $testKeyspace.$table_name (
         |id int,
         |name frozen<fullname>,
         |PRIMARY KEY (id));
         |""".stripMargin

    session.execute(udt)
    session.execute(table)
  }
}
