package com.datastax.gatling.plugin.simulations.cql

import java.nio.ByteBuffer
import java.sql.Timestamp

import com.datastax.gatling.plugin.CqlPredef._
import com.datastax.gatling.plugin.base.BaseCqlSimulation
import com.datastax.oss.driver.api.core.`type`.{DataType, DataTypes, TupleType}
import com.datastax.oss.driver.api.core.cql.ResultSet
import com.datastax.oss.driver.api.core.uuid.Uuids
import io.gatling.core.Predef._

import scala.concurrent.duration.DurationInt

class BoundCqlTypesSimulation extends BaseCqlSimulation {

  val table_name = "test_table_types"
  val table_counter_name = "test_table_counter"

  createTestKeyspace
  createTable

  val cqlConfig = cql.session(session)
  val udtType = session.getMetadata.getKeyspace(testKeyspace).get().getUserDefinedType("fullname")

  val insertFullName = udtType.get().newValue()
      .setString("firstname", "John")
      .setString("lastname", "Smith")

  val tupleType = DataTypes.tupleOf(DataTypes.TEXT, DataTypes.TEXT)
  val insertTuple = tupleType.newValue("one", "two")

  val uuid = Uuids.random()

  val preparedStatementInsert =
    s"""INSERT INTO $testKeyspace.$table_name (
       |uuid_type, timeuuid_type, int_type,
       |text_type, float_type, double_type, decimal_type, boolean_type, inet_type, timestamp_type,
       |bigint_type, blob_type, list_type, set_type, map_type, date_type, varint_type, smallint_type,
       |tinyint_type, time_type, null_type, udt_type, tuple_type, frozen_set_type, set_string_type)
       |VALUES (
       |:uuid_type, :timeuuid_type, :int_type,
       |:text_type, :float_type, :double_type, :decimal_type, :boolean_type, :inet_type, :timestamp_type,
       |:bigint_type, :blob_type, :list_type, :set_type, :map_type, :date_type, :varint_type, :smallint_type,
       |:tinyint_type, :time_type, :null_type, :udt_type, :tuple_type, :frozen_set_type, :set_string_type
       |)""".stripMargin

  val preparedStatementSelect = s"""SELECT * FROM $testKeyspace.$table_name WHERE uuid_type = ?"""

  val preparedInsert = session.prepare(preparedStatementInsert)
  val preparedSelect = session.prepare(preparedStatementSelect)

  val insertPreparedCql = cql("Insert Prepared Statements")
      .executeNamed(preparedInsert)

  val selectPreparedCql = cql("Select Prepared Statement")
      .executePrepared(preparedSelect)

  val preparedFeed = Iterator.continually(
    Map(
      "uuid_type" -> uuid,
      "timeuuid_type" -> Uuids.timeBased(),
      "int_type" -> 1,
      "text_type" -> "text",
      "float_type" -> 4.50,
      "double_type" -> 1.0000000000000002,
      "decimal_type" -> 26.0,
      "boolean_type" -> true,
      "inet_type" -> "127.0.0.1",
      "timestamp_type" -> getRandomEpoch,
      "bigint_type" -> 1483060072432L,
      "blob_type" -> ByteBuffer.wrap(Array[Byte](192.toByte, 168.toByte, 1, 9)),
      "list_type" -> List(1, 2),
      "set_type" -> Set(1, 2),
      "map_type" -> Map(1 -> 2),
      "date_type" -> "2015-05-03",
      "varint_type" -> "544",
      "smallint_type" -> 1,
      "tinyint_type" -> 1,
      "time_type" -> "13:30:54.234",
      "udt_type" -> insertFullName,
      "tuple_type" -> insertTuple,
      "frozen_set_type" -> Set(1, 2),
      "set_string_type" -> Seq("test", "me")
    )
  )


  // Start counter details
  val preparedCounterStatementInsert =
    s"""UPDATE $testKeyspace.$table_counter_name
       | SET counter_type = counter_type + :counter_type
       | WHERE uuid_type = :uuid_type""".stripMargin

  val preparedCounterStatementSelect = s"""SELECT * FROM $testKeyspace.$table_counter_name WHERE uuid_type = ?"""

  val preparedCounterInsert = session.prepare(preparedCounterStatementInsert)
  val preparedCounterSelect = session.prepare(preparedCounterStatementSelect)

  val insertCounterPreparedCql = cql("Insert Counter Prepared Statements")
      .executeNamed(preparedCounterInsert)

  val selectCounterPreparedCql = cql("Select Counter Prepared Statement")
      .executePrepared(preparedCounterSelect)

  val counterFeed = Iterator.continually(
    Map(
      "uuid_type" -> uuid,
      "counter_type" -> 2
    )
  )
  // End counter details


  val scn = scenario("BoundCqlStatement")

      .feed(preparedFeed)
      .exec(insertPreparedCql
          .check(exhausted is true)
          .check(rowCount is 0) // "normal" INSERTs don't return anything
          .build()
      )
      .pause(100.millis)

      .exec(selectPreparedCql
          .withParams(List("uuid_type"))

          .check(rowCount is 1)
          .check(columnValue("name") not "")
          .check(columnValue("null_type") not "test")
          .check(columnValue("boolean_type") is true)
          .build()
      )
      .pause(100.millis)

      .feed(counterFeed)
      .exec(insertCounterPreparedCql
          .check(exhausted is true)
          .check(rowCount is 0) // "normal" INSERTs don't return anything
          .build()
      )
      .pause(100.millis)

      .exec(selectCounterPreparedCql
          .withParams(List("uuid_type"))
          .check(rowCount is 1)
          .check(columnValue("counter_type") is 2)
          .build()
      )
      .pause(100.millis)


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
      s"""create table if not exists $testKeyspace.$table_name (
         |uuid_type uuid,
         |timeuuid_type timeuuid,
         |int_type int,
         |text_type text,
         |float_type float,
         |double_type double,
         |decimal_type decimal,
         |boolean_type boolean,
         |inet_type inet,
         |timestamp_type timestamp,
         |bigint_type bigint,
         |blob_type blob,
         |varint_type varint,
         |list_type list<int>,
         |set_type set<int>,
         |map_type map<int,int>,
         |date_type date,
         |smallint_type smallint,
         |tinyint_type tinyint,
         |time_type time,
         |null_type text,
         |udt_type frozen<fullname>,
         |tuple_type tuple<text, text>,
         |frozen_set_type frozen<set<int>>,
         |set_string_type set<text>,
         |PRIMARY KEY (uuid_type));
         |""".stripMargin

    val table_counter =
      s"""
         |create table if not exists $testKeyspace.$table_counter_name (
         |uuid_type uuid,
         |counter_type counter,
         |PRIMARY KEY (uuid_type));
       """.stripMargin

    session.execute(udt)
    session.execute(table)
    session.execute(table_counter)
  }


  def getRandomEpoch: Timestamp = {
    val offset: Long = Timestamp.valueOf("2012-01-01 00:00:00").getTime
    val end = Timestamp.valueOf("2017-01-01 00:00:00").getTime
    val diff = end - offset + 1
    val time: Long = (offset + (Math.random() * diff)).toLong
    new Timestamp(time)
  }

}
