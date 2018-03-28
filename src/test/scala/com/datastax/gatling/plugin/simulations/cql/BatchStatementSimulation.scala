package com.datastax.gatling.plugin.simulations.cql

import com.datastax.driver.core.ResultSet
import com.datastax.gatling.plugin.DsePredef._
import com.datastax.gatling.plugin.base.BaseCqlSimulation
import io.gatling.core.Predef._

import scala.concurrent.duration.DurationInt

class BatchStatementSimulation extends BaseCqlSimulation {

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

  val preparedStatementInsert = s"""INSERT INTO $testKeyspace.$table_name (id, name) VALUES (?, ?)"""
  val preparedInsert = session.prepare(preparedStatementInsert)

  val insertPreparedCql = cql("Insert Prepared Batch Statements")
      .executePreparedBatch(Array(preparedInsert, preparedInsert))

  val preparedFeed = Iterator.continually(
    Map(
      "id" -> preparedId,
      "fullname" -> insertFullName
    )
  )

  val scn = scenario("BatchStatement")
      .feed(preparedFeed)
      .exec(insertPreparedCql
          .check(exhausted is true)
          .check(rowCount is 0) // "normal" INSERTs don't return anything
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
