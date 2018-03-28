package com.datastax.gatling.plugin.simulations.cql

import com.datastax.driver.core.ResultSet
import com.datastax.gatling.plugin.DsePredef._
import com.datastax.gatling.plugin.base.BaseCqlSimulation
import io.gatling.core.Predef._

import scala.concurrent.duration.DurationInt

class UdtStatementSimulation extends BaseCqlSimulation {

  val table_name = "test_table_udt"
  val udt_name = "fullname"

  createTestKeyspace
  createTable

  val cqlConfig = cql.session(session)
  //Initialize Gatling DSL with your session
  val addressType = session.getCluster.getMetadata.getKeyspace(testKeyspace).getUserType("fullname")

  val simpleId = 1
  val preparedId = 2
  val namedId = 3
  val insertFullName = addressType.newValue()
      .setString("firstname", "John")
      .setString("lastname", "Smith")


  val simpleStatementInsert = s"""INSERT INTO $testKeyspace.$table_name (id, name) VALUES ($simpleId, $insertFullName)"""
  val simpleStatementSelect = s"""SELECT * FROM $testKeyspace.$table_name WHERE id = $simpleId"""

  val insertCql = cql("Insert Simple Statements")
      .executeCql(simpleStatementInsert)

  val selectCql = cql("Select Simple Statement")
      .executeCql(simpleStatementSelect)

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

  val scn = scenario("SimpleStatement")
      .exec(insertCql
          .check(exhausted is true)
          .check(rowCount is 0) // "normal" INSERTs don't return anything
      )
      .pause(100.millis)

      .exec(selectCql
          .check(rowCount is 1)
          .check(columnValue("name").find(0) not "")
      )
      .pause(100.millis)

      .feed(preparedFeed)
      .exec(insertPreparedCql
          .withParams(List("id", "fullname"))
          .check(exhausted is true)
          .check(rowCount is 0) // "normal" INSERTs don't return anything
      )
      .pause(100.millis)

      .exec(selectPreparedCql
          .withParams(List("id"))
          .check(rowCount is 1)
          .check(columnValue("name").find(0) not "")
      )
      .pause(100.millis)

      .feed(namedFeed)
      .exec(insertNamedCql
          .check(exhausted is true)
          .check(rowCount is 0) // "normal" INSERTs don't return anything
      )
      .pause(100.millis)

      .exec(selectNamedCql
          .check(rowCount is 1)
          .check(columnValue("name").find(0) not "")
      )

  setUp(
    scn.inject(constantUsersPerSec(1) during 1.seconds).protocols(cqlConfig)
  ).assertions(
    global.failedRequests.count.is(0)
  )


  def createTable: ResultSet = {

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
