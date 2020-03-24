package com.datastax.gatling.plugin.simulations.cql

import com.datastax.gatling.plugin.DsePredef._
import com.datastax.gatling.plugin.base.BaseCqlSimulation
import com.datastax.oss.driver.api.core.`type`.DataTypes
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.querybuilder.{QueryBuilder, SchemaBuilder}
import io.gatling.core.Predef._

import scala.concurrent.duration.DurationInt

class SimpleStatementSimulation extends BaseCqlSimulation {

  val table_name = "test_table_simple"

  createTestKeyspace

  val createTable = SchemaBuilder.createTable(testKeyspace, table_name)
    .ifNotExists
    .withPartitionKey("id",DataTypes.INT)
    .withClusteringColumn("str", DataTypes.TEXT)
    .build
  session.execute(createTable)

  val dseProtocol = dseProtocolBuilder.session(session) //Initialize Gatling DSL with your session

  val insertId = 1
  val insertStr = "one"

  val simpleStatementInsert = QueryBuilder.insertInto(testKeyspace, table_name)
    .value("id", QueryBuilder.literal(insertId))
    .value("str", QueryBuilder.literal(insertStr))
    .build()
  val simpleStatementSelect = QueryBuilder.selectFrom(testKeyspace, table_name)
    .all()
    .whereColumn("id").isEqualTo(QueryBuilder.literal(insertId))
    .build()

  private def selectCqlExtract(row:Row):String =
    row.getString("str")

  val scn = scenario("SimpleStatement")
      .exec(
        cql("Insert_Statement")
          .executeStatement(simpleStatementInsert)
          .check(resultSet.transform(_.hasMorePages) is false)
          .check(resultSet.transform(_.remaining()) is 0) // "normal" INSERTs don't return anything
      )
      .pause(1.seconds)
      .group("TestGroup") {
          exec(
            cql("Select_Statement")
              .executeStatement(simpleStatementSelect)
              .check(resultSet.transform(_.getExecutionInfo.getWarnings.size).is(0))
              .check(resultSet.transform(_.remaining()) is 1)
              .check(resultSet.transform(rs => selectCqlExtract(rs.one)) is insertStr))
      }

  setUp(
    scn.inject(constantUsersPerSec(150) during 1.seconds).protocols(dseProtocol)
  ).assertions(
    global.failedRequests.count.is(0)
  )
}
