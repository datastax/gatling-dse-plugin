package com.datastax.gatling.plugin.utils

import java.math.BigInteger
import java.net.InetAddress
import java.nio.ByteBuffer

import com.datastax.driver.core.utils.UUIDs
import com.datastax.driver.core.{DataType, _}
import com.datastax.driver.dse.geometry._
import com.datastax.gatling.plugin.base.BaseCassandraServerSpec
import com.datastax.gatling.plugin.exceptions.CqlTypeException
import com.github.nscala_time.time.Imports.DateTime
import io.gatling.core.session.Session

import scala.collection.JavaConverters._

class CqlPreparedStatementUtilSpec extends BaseCassandraServerSpec {

  val keyspace = "gatling_cql_unittests"

  createKeyspace(keyspace)

  val gatlingSession = new Session("test", 1L)
  val defaultSessionVars = Map(
    "string" -> "string",
    "int" -> 12,
    "intStr" -> "12",
    "long" -> 12L,
    "longStr" -> "1483299340813L",

    "boolean" -> true,
    "booleanStrTrue" -> "true",
    "booleanStrFalse" -> "false",
    "booleanStrInvalid" -> "yes",
    "booleanIntTrue" -> 1,
    "booleanIntFalse" -> 0,

    "float" -> 12.0.toFloat,
    "floatStr" -> "12.4",
    "double" -> 12.0,
    "epoch" -> 1483299340813L,
    "number" -> 12.asInstanceOf[Number],

    "inetStr" -> "127.0.0.1",
    "inet" -> InetAddress.getByName("127.0.0.1"),

    "localDate" -> LocalDate.fromMillisSinceEpoch(1483299340813L),
    "stringDate" -> "2016-10-05",

    "set" -> Set(1),
    "setString" -> Set("test"),
    "setJava" -> Set(1).asJava,

    "list" -> List(1),
    "listJava" -> List(1).asJava,

    "map" -> Map(1 -> 1),
    "mapJava" -> Map(1 -> 1).asJava,

    "seq" -> Seq(1),
    "seqString" -> Seq("test"),


    "bigDecimal" -> BigDecimal(1).bigDecimal,
    "bigDecimalStr" -> "1.01",

    "bigInteger" -> BigInteger.valueOf(12L),
    "javaNumber" -> 12.asInstanceOf[Number],

    "javaDate" -> new java.util.Date(),
    "isoDateString" -> "2008-03-01T13:00:00Z",
    "dateString" -> "2016-10-05",

    "uuid" -> UUIDs.random(),
    "uuidString" -> "252a3806-b8be-42d3-929d-4cbb380a433e",
    "timeUuid" -> UUIDs.timeBased(),

    "byte" -> 12.toByte,
    "short" -> 12.toShort,

    "byteBuffer" -> ByteBuffer.wrap(Array(12.toByte)),
    "byteArray" -> Array(12.toByte),

    "hourTime" -> "01:01:01",
    "nanoTime" -> "01:01:01.343",

    "none_type" -> None,

    "null_type" -> null,
    
    "point_type" -> new Point(1.0, 1.0),
    "linestring_type" -> new LineString(new Point(1.0, 1.0), new Point(2.0, 2.0)),
    "polygon_type" -> new Polygon(new Point(1.0, 1.0), new Point(2.0, 2.0), new Point(3.0, 3.0), new Point(4.0, 4.0))
  )

  val defaultGatlingSession: Session = gatlingSession.setAll(defaultSessionVars)


  describe("preparedStatements") {

    val table = "prep"
    createTable(keyspace, table, "id int, str text, PRIMARY KEY (id)")

    describe("getParamsList") {

      it("should return a list of types") {

        val preparedStatement = dseSession.prepare(s"SELECT * FROM $keyspace.$table where id = ?")
        val paramList = CqlPreparedStatementUtil.getParamsList(preparedStatement)

        paramList should contain(DataType.Name.INT)
      }

    }

    describe("getParamsMap") {

      it("should return a map of types") {

        val preparedStatement = dseSession.prepare(s"SELECT * FROM $keyspace.$table where id = :id")
        val paramsMap = CqlPreparedStatementUtil.getParamsMap(preparedStatement)

        paramsMap("id") shouldBe DataType.Name.INT
      }

    }
  }


  describe("typeConversion") {

    describe("asInt") {

      it("should accept a native int") {
        CqlPreparedStatementUtil.asInteger(defaultGatlingSession, "int") shouldBe a[java.lang.Integer]
      }

      it("should accept a String") {
        CqlPreparedStatementUtil.asInteger(defaultGatlingSession, "intStr") shouldBe a[java.lang.Integer]
        CqlPreparedStatementUtil.asInteger(defaultGatlingSession, "intStr") shouldBe
            defaultSessionVars("intStr").asInstanceOf[String].toInt
      }

      it("should not accept a long and produce a CqlTypeException") {
        intercept[CqlTypeException] {
          CqlPreparedStatementUtil.asInteger(defaultGatlingSession, "long")
        }
      }
    }


    describe("asString") {

      it("should accept a native String") {
        CqlPreparedStatementUtil.asString(defaultGatlingSession, "string") shouldBe a[String]
        CqlPreparedStatementUtil.asString(defaultGatlingSession, "string") shouldBe defaultSessionVars("string")
      }

      it("should not accept a long and produce a CqlTypeException") {
        intercept[CqlTypeException] {
          CqlPreparedStatementUtil.asString(defaultGatlingSession, "long") shouldBe a[String]
        }
      }
    }
    
    describe("asPoint") {

      it("should accept a Point") {
        CqlPreparedStatementUtil.asPoint(defaultGatlingSession, "point_type") shouldBe a[Point]
        CqlPreparedStatementUtil.asPoint(defaultGatlingSession, "point_type") shouldBe defaultSessionVars("point_type").asInstanceOf[Point]
      }

      it("should not accept a long and produce a CqlTypeException") {
        intercept[CqlTypeException] {
          CqlPreparedStatementUtil.asPoint(defaultGatlingSession, "long") shouldBe a[Point]
        }
      }
    }
    
    describe("asLineString") {

      it("should accept a LineString") {
        CqlPreparedStatementUtil.asLineString(defaultGatlingSession, "linestring_type") shouldBe a[LineString]
        CqlPreparedStatementUtil.asLineString(defaultGatlingSession, "linestring_type") shouldBe defaultSessionVars("linestring_type").asInstanceOf[LineString]
      }

      it("should not accept a long and produce a CqlTypeException") {
        intercept[CqlTypeException] {
          CqlPreparedStatementUtil.asLineString(defaultGatlingSession, "long") shouldBe a[LineString]
        }
      }
    }
    
    describe("asPolygon") {

      it("should accept a Polygon") {
        CqlPreparedStatementUtil.asPolygon(defaultGatlingSession, "polygon_type") shouldBe a[Polygon]
        CqlPreparedStatementUtil.asPolygon(defaultGatlingSession, "polygon_type") shouldBe defaultSessionVars("polygon_type").asInstanceOf[Polygon]
      }

      it("should not accept a long and produce a CqlTypeException") {
        intercept[CqlTypeException] {
          CqlPreparedStatementUtil.asPolygon(defaultGatlingSession, "long") shouldBe a[Polygon]
        }
      }
    }

    describe("asUuid") {

      it("should accept a Random UUID") {
        CqlPreparedStatementUtil.asUuid(defaultGatlingSession, "uuid") shouldBe a[java.util.UUID]
        CqlPreparedStatementUtil.asUuid(defaultGatlingSession, "uuid") shouldBe
            defaultSessionVars("uuid").asInstanceOf[java.util.UUID]
      }

      it("should accept a Time UUID") {
        CqlPreparedStatementUtil.asUuid(defaultGatlingSession, "timeUuid") shouldBe a[java.util.UUID]
        CqlPreparedStatementUtil.asUuid(defaultGatlingSession, "timeUuid") shouldBe
            defaultSessionVars("timeUuid").asInstanceOf[java.util.UUID]
      }

      it("should accept a String UUID") {
        CqlPreparedStatementUtil.asUuid(defaultGatlingSession, "uuidString") shouldBe a[java.util.UUID]
        CqlPreparedStatementUtil.asUuid(defaultGatlingSession, "uuidString") shouldBe
            java.util.UUID.fromString(defaultSessionVars("uuidString").asInstanceOf[String])
      }

      it("should not accept a long and produce a CqlTypeException") {
        intercept[CqlTypeException] {
          CqlPreparedStatementUtil.asUuid(defaultGatlingSession, "long") shouldBe a[java.util.UUID]
        }
      }
    }


    describe("asBoolean") {

      it("should accept boolean") {
        CqlPreparedStatementUtil.asBoolean(defaultGatlingSession, "boolean") shouldBe a[java.lang.Boolean]
        CqlPreparedStatementUtil.asBoolean(defaultGatlingSession, "boolean") shouldBe defaultSessionVars("boolean")
      }

      it("should accept string of 'true' or 'false'") {
        CqlPreparedStatementUtil.asBoolean(defaultGatlingSession, "booleanStrFalse") shouldBe a[java.lang.Boolean]
        CqlPreparedStatementUtil.asBoolean(defaultGatlingSession, "booleanStrFalse") shouldBe false

        CqlPreparedStatementUtil.asBoolean(defaultGatlingSession, "booleanStrTrue") shouldBe a[java.lang.Boolean]
        CqlPreparedStatementUtil.asBoolean(defaultGatlingSession, "booleanStrTrue") shouldBe true
      }

      it("should accept int of 0 or 1") {
        CqlPreparedStatementUtil.asBoolean(defaultGatlingSession, "booleanIntTrue") shouldBe a[java.lang.Boolean]
        CqlPreparedStatementUtil.asBoolean(defaultGatlingSession, "booleanIntTrue") shouldBe true

        CqlPreparedStatementUtil.asBoolean(defaultGatlingSession, "booleanIntFalse") shouldBe a[java.lang.Boolean]
        CqlPreparedStatementUtil.asBoolean(defaultGatlingSession, "booleanIntFalse") shouldBe false
      }

      it("should not accept string of 'yes'") {
        intercept[CqlTypeException] {
          CqlPreparedStatementUtil.asBoolean(defaultGatlingSession, "booleanStrInvalid") shouldBe a[java.lang.Boolean]
        }
      }

      it("should not accept a long and produce a CqlTypeException") {
        intercept[CqlTypeException] {
          CqlPreparedStatementUtil.asBoolean(defaultGatlingSession, "long") shouldBe a[java.lang.Boolean]
        }
      }
    }


    describe("asFloat") {

      it("should accept a Float") {
        CqlPreparedStatementUtil.asFloat(defaultGatlingSession, "float") shouldBe a[java.lang.Float]
        CqlPreparedStatementUtil.asFloat(defaultGatlingSession, "float") shouldBe defaultSessionVars("float")
      }

      it("should accept a Double") {
        CqlPreparedStatementUtil.asFloat(defaultGatlingSession, "double") shouldBe a[java.lang.Float]
        CqlPreparedStatementUtil.asFloat(defaultGatlingSession, "double") shouldBe
            defaultSessionVars("double").asInstanceOf[Double].toFloat
      }

      it("should accept a Int") {
        CqlPreparedStatementUtil.asFloat(defaultGatlingSession, "int") shouldBe a[java.lang.Float]
        CqlPreparedStatementUtil.asFloat(defaultGatlingSession, "int") shouldBe
            defaultSessionVars("int").asInstanceOf[Int].toFloat

      }

      it("should accept a String") {
        CqlPreparedStatementUtil.asFloat(defaultGatlingSession, "floatStr") shouldBe a[java.lang.Float]
        CqlPreparedStatementUtil.asFloat(defaultGatlingSession, "floatStr") shouldBe 12.4.toFloat
      }

      it("should not accept a long and produce a CqlTypeException") {
        intercept[CqlTypeException] {
          CqlPreparedStatementUtil.asFloat(defaultGatlingSession, "long") shouldBe a[java.lang.Float]
        }
      }
    }


    describe("asDecimal") {

      it("should accept a Double") {
        CqlPreparedStatementUtil.asDecimal(defaultGatlingSession, "double") shouldBe a[java.math.BigDecimal]
        CqlPreparedStatementUtil.asDecimal(defaultGatlingSession, "double") shouldBe
            BigDecimal(defaultSessionVars("double").asInstanceOf[Double]).bigDecimal
      }

      it("should accept a long") {
        CqlPreparedStatementUtil.asDecimal(defaultGatlingSession, "long") shouldBe a[java.math.BigDecimal]
        CqlPreparedStatementUtil.asDecimal(defaultGatlingSession, "long") shouldBe
            BigDecimal(defaultSessionVars("long").asInstanceOf[Long]).bigDecimal
      }

      it("should accept a BigDecimal") {
        CqlPreparedStatementUtil.asDecimal(defaultGatlingSession, "bigDecimal") shouldBe a[java.math.BigDecimal]
        CqlPreparedStatementUtil.asDecimal(defaultGatlingSession, "bigDecimal") shouldBe
            defaultSessionVars("bigDecimal").asInstanceOf[java.math.BigDecimal]
      }

      it("should accept a String") {
        CqlPreparedStatementUtil.asDecimal(defaultGatlingSession, "bigDecimalStr") shouldBe a[java.math.BigDecimal]
        CqlPreparedStatementUtil.asDecimal(defaultGatlingSession, "bigDecimalStr") shouldBe
            BigDecimal(defaultSessionVars("bigDecimalStr").asInstanceOf[String]).bigDecimal
      }

      it("should not accept a uuid and produce a CqlTypeException") {
        intercept[CqlTypeException] {
          CqlPreparedStatementUtil.asDecimal(defaultGatlingSession, "uuid") shouldBe a[java.math.BigDecimal]
        }
      }
    }


    describe("asDouble") {

      it("should accept a Double") {
        CqlPreparedStatementUtil.asDouble(defaultGatlingSession, "double") shouldBe a[java.lang.Double]
        CqlPreparedStatementUtil.asDouble(defaultGatlingSession, "double") shouldBe defaultSessionVars("double")
      }

      it("should accept a Int") {
        CqlPreparedStatementUtil.asDouble(defaultGatlingSession, "int") shouldBe a[java.lang.Double]
        CqlPreparedStatementUtil.asDouble(defaultGatlingSession, "int") shouldBe
            defaultSessionVars("int").asInstanceOf[Int].toDouble
      }

      it("should accept a String") {
        CqlPreparedStatementUtil.asDouble(defaultGatlingSession, "floatStr") shouldBe a[java.lang.Double]
        CqlPreparedStatementUtil.asDouble(defaultGatlingSession, "floatStr") shouldBe
            defaultSessionVars("floatStr").asInstanceOf[String].toFloat.toDouble
      }

      it("should not accept a long and produce a CqlTypeException") {
        intercept[CqlTypeException] {
          CqlPreparedStatementUtil.asDouble(defaultGatlingSession, "long") shouldBe a[java.lang.Double]
        }
      }
    }

    describe("asCounter") {

      it("should accept a native long") {
        CqlPreparedStatementUtil.asCounter(defaultGatlingSession, "long") shouldBe a[java.lang.Long]
        CqlPreparedStatementUtil.asCounter(defaultGatlingSession, "long") shouldBe defaultSessionVars("long")
      }

      it("should accept an int") {
        CqlPreparedStatementUtil.asCounter(defaultGatlingSession, "int") shouldBe a[java.lang.Long]
        CqlPreparedStatementUtil.asCounter(defaultGatlingSession, "int") shouldBe
            defaultSessionVars("int").asInstanceOf[Int].toLong
      }

      it("should not accept a float and produce a CqlTypeException") {
        intercept[CqlTypeException] {
          CqlPreparedStatementUtil.asCounter(defaultGatlingSession, "float") shouldBe a[java.lang.Long]
        }
      }

    }

    describe("asDate") {

      it("should accept a date string") {
        CqlPreparedStatementUtil.asDate(defaultGatlingSession, "stringDate") shouldBe a[LocalDate]
        CqlPreparedStatementUtil.asDate(defaultGatlingSession, "stringDate").getDay.equals(5)
      }

      it("should accept a long") {
        CqlPreparedStatementUtil.asDate(defaultGatlingSession, "long") shouldBe a[LocalDate]
        CqlPreparedStatementUtil.asDate(defaultGatlingSession, "long").getDay.equals(1)
      }

      it("should accept an int") {
        CqlPreparedStatementUtil.asDate(defaultGatlingSession, "int") shouldBe a[LocalDate]
        CqlPreparedStatementUtil.asDate(defaultGatlingSession, "int").getDay.equals(13)
      }

      it("should accept an native localDate") {
        CqlPreparedStatementUtil.asDate(defaultGatlingSession, "localDate") shouldBe a[LocalDate]
        CqlPreparedStatementUtil.asDate(defaultGatlingSession, "localDate").getDay.equals(1)
      }

      it("should not accept a float and produce a CqlTypeException") {
        intercept[CqlTypeException] {
          CqlPreparedStatementUtil.asDate(defaultGatlingSession, "float") shouldBe a[LocalDate]
        }
      }

    }


    describe("asSet") {

      it("should accept a scala set") {
        CqlPreparedStatementUtil.asSet(defaultGatlingSession, "set") shouldBe a[java.util.Set[_]]
        CqlPreparedStatementUtil.asSet(defaultGatlingSession, "set") shouldBe
            defaultSessionVars("set").asInstanceOf[Set[Int]].asJava
      }

      it("should accept a java set") {
        CqlPreparedStatementUtil.asSet(defaultGatlingSession, "setJava") shouldBe a[java.util.Set[_]]
        CqlPreparedStatementUtil.asSet(defaultGatlingSession, "setJava") shouldBe
            defaultSessionVars("set").asInstanceOf[Set[Int]].asJava
      }

      it("should accept a scala seq of ints") {
        CqlPreparedStatementUtil.asSet(defaultGatlingSession, "seq") shouldBe a[java.util.Set[_]]
        CqlPreparedStatementUtil.asSet(defaultGatlingSession, "seq") shouldBe
            defaultSessionVars("set").asInstanceOf[Set[Int]].asJava
      }

      it("should accept a scala seq of strings") {
        CqlPreparedStatementUtil.asSet(defaultGatlingSession, "seqString") shouldBe a[java.util.Set[_]]
        CqlPreparedStatementUtil.asSet(defaultGatlingSession, "seqString") shouldBe
            defaultSessionVars("setString").asInstanceOf[Set[String]].asJava
      }

      it("should not accept a float and produce a CqlTypeException") {
        intercept[CqlTypeException] {
          CqlPreparedStatementUtil.asSet(defaultGatlingSession, "float") shouldBe a[java.util.Set[_]]
        }
      }

    }


    describe("asList") {

      it("should accept a scala set") {
        CqlPreparedStatementUtil.asList(defaultGatlingSession, "list") shouldBe a[java.util.List[_]]
        CqlPreparedStatementUtil.asList(defaultGatlingSession, "list") shouldBe
            defaultSessionVars("list").asInstanceOf[List[Int]].asJava
      }

      it("should accept a java set") {
        CqlPreparedStatementUtil.asList(defaultGatlingSession, "listJava") shouldBe a[java.util.List[_]]
      }

      it("should accept a scala seq") {
        CqlPreparedStatementUtil.asList(defaultGatlingSession, "seq") shouldBe a[java.util.List[_]]
        CqlPreparedStatementUtil.asList(defaultGatlingSession, "seq") shouldBe
            defaultSessionVars("list").asInstanceOf[List[Int]].asJava
      }

      it("should not accept a float and produce a CqlTypeException") {
        intercept[CqlTypeException] {
          CqlPreparedStatementUtil.asList(defaultGatlingSession, "float") shouldBe a[java.util.List[_]]
        }
      }

    }


    describe("asMap") {

      it("should accept a scala set") {
        CqlPreparedStatementUtil.asMap(defaultGatlingSession, "map") shouldBe a[java.util.Map[_, _]]
        CqlPreparedStatementUtil.asMap(defaultGatlingSession, "map") shouldBe
            defaultSessionVars("map").asInstanceOf[Map[Int, Int]].asJava
      }

      it("should accept a java set") {
        CqlPreparedStatementUtil.asMap(defaultGatlingSession, "mapJava") shouldBe a[java.util.Map[_, _]]
      }

      it("should not accept a float and produce a CqlTypeException") {
        intercept[CqlTypeException] {
          CqlPreparedStatementUtil.asMap(defaultGatlingSession, "float").get(1) shouldBe 1
        }
      }

    }

    describe("asInet") {

      it("should accept a string") {
        CqlPreparedStatementUtil.asInet(defaultGatlingSession, "inet") shouldBe a[InetAddress]
        CqlPreparedStatementUtil.asInet(defaultGatlingSession, "inet").getHostName shouldBe "localhost"
      }

      it("should accept a long") {
        CqlPreparedStatementUtil.asInet(defaultGatlingSession, "inetStr") shouldBe a[InetAddress]
        CqlPreparedStatementUtil.asInet(defaultGatlingSession, "inetStr").getHostName shouldBe "localhost"
      }

      it("should not accept a float and produce a CqlTypeException") {
        intercept[CqlTypeException] {
          CqlPreparedStatementUtil.asInet(defaultGatlingSession, "float") shouldBe a[InetAddress]
        }
      }

    }

    describe("asTime") {

      it("should accept a Long") {
        CqlPreparedStatementUtil.asTime(defaultGatlingSession, "long") shouldBe a[java.lang.Long]
      }

      describe("should accept a String") {

        it("should accept a String time w/o nanoseconds") {
          val validHour = CqlPreparedStatementUtil.asTime(defaultGatlingSession, "hourTime")
          validHour shouldBe a[java.lang.Long]
          validHour shouldBe 3661000000000L
        }

        it("should accept a String time w/ nanoseconds") {
          val validNano = CqlPreparedStatementUtil.asTime(defaultGatlingSession, "nanoTime")
          validNano shouldBe a[java.lang.Long]
          validNano shouldBe 3661343000000L
        }

        it("should not accept invalid hour and produce a CqlTypeException") {
          val newSessionVars = Map("invalidHour" -> "43:04:32")
          val newSession: Session = gatlingSession.setAll(newSessionVars)
          intercept[CqlTypeException] {
            CqlPreparedStatementUtil.asTime(newSession, "invalidHour") shouldBe a[java.lang.Long]
          }
        }

        it("should not accept invalid minute and produce a CqlTypeException") {
          val newSessionVars = Map("invalidMin" -> "01:78:32")
          val newSession: Session = gatlingSession.setAll(newSessionVars)
          intercept[CqlTypeException] {
            CqlPreparedStatementUtil.asTime(newSession, "invalidMin") shouldBe a[java.lang.Long]
          }
        }

        it("should not accept invalid seconds and produce a CqlTypeException") {
          val newSessionVars = Map("invalidSec" -> "01:01:78")
          val newSession: Session = gatlingSession.setAll(newSessionVars)
          intercept[CqlTypeException] {
            CqlPreparedStatementUtil.asTime(newSession, "invalidSec") shouldBe a[Long]
          }
        }

        it("should not accept invalid nanoseconds and produce a CqlTypeException") {
          val newSessionVars = Map("invalidNano" -> "01:01:01.11111111111")
          val newSession: Session = gatlingSession.setAll(newSessionVars)
          intercept[CqlTypeException] {
            CqlPreparedStatementUtil.asTime(newSession, "invalidNano") shouldBe a[Long]
          }
        }

      }

      it("should not accept a float and produce a CqlTypeException") {
        intercept[CqlTypeException] {
          CqlPreparedStatementUtil.asTime(defaultGatlingSession, "float") shouldBe a[Long]
        }
      }
    }


    describe("asTinyInt") {

      it("should accept an int") {
        CqlPreparedStatementUtil.asTinyInt(defaultGatlingSession, "int") shouldBe a[java.lang.Byte]
        CqlPreparedStatementUtil.asTinyInt(defaultGatlingSession, "int") shouldBe
            defaultSessionVars("int").asInstanceOf[Int].byteValue()
      }

      it("should accept an byte") {
        CqlPreparedStatementUtil.asTinyInt(defaultGatlingSession, "byte") shouldBe a[java.lang.Byte]
        CqlPreparedStatementUtil.asTinyInt(defaultGatlingSession, "byte") shouldBe
            defaultSessionVars("byte").asInstanceOf[Byte]
      }

      it("should accept a string") {
        CqlPreparedStatementUtil.asTinyInt(defaultGatlingSession, "intStr") shouldBe a[java.lang.Byte]
        CqlPreparedStatementUtil.asTinyInt(defaultGatlingSession, "intStr") shouldBe
            defaultSessionVars("intStr").asInstanceOf[String].toInt.toByte
      }

      it("should not accept a float and produce a CqlTypeException") {
        intercept[CqlTypeException] {
          CqlPreparedStatementUtil.asTinyInt(defaultGatlingSession, "float") shouldBe a[java.lang.Byte]
        }
      }

    }

    describe("asSmallInt") {

      it("should accept an int") {
        CqlPreparedStatementUtil.asSmallInt(defaultGatlingSession, "int") shouldBe a[java.lang.Short]
        CqlPreparedStatementUtil.asSmallInt(defaultGatlingSession, "int") shouldBe
            defaultSessionVars("int").asInstanceOf[Int].toShort
      }

      it("should accept an short") {
        CqlPreparedStatementUtil.asSmallInt(defaultGatlingSession, "short") shouldBe a[java.lang.Short]
        CqlPreparedStatementUtil.asSmallInt(defaultGatlingSession, "short") shouldBe
            defaultSessionVars("short").asInstanceOf[Short]
      }

      it("should accept a string") {
        CqlPreparedStatementUtil.asSmallInt(defaultGatlingSession, "intStr") shouldBe a[java.lang.Short]
        CqlPreparedStatementUtil.asSmallInt(defaultGatlingSession, "intStr") shouldBe
            defaultSessionVars("intStr").asInstanceOf[String].toInt.toShort
      }

      it("should not accept a float and produce a CqlTypeException") {
        intercept[CqlTypeException] {
          CqlPreparedStatementUtil.asSmallInt(defaultGatlingSession, "float") shouldBe a[java.lang.Short]
        }
      }

    }

    describe("asVarInt") {

      it("should accept an int") {
        CqlPreparedStatementUtil.asVarInt(defaultGatlingSession, "int") shouldBe a[BigInteger]
        CqlPreparedStatementUtil.asVarInt(defaultGatlingSession, "int") shouldBe
            BigInteger.valueOf(defaultSessionVars("int").asInstanceOf[Int])
      }

      it("should accept an long") {
        CqlPreparedStatementUtil.asVarInt(defaultGatlingSession, "long") shouldBe a[BigInteger]
        CqlPreparedStatementUtil.asVarInt(defaultGatlingSession, "long") shouldBe
            BigInteger.valueOf(defaultSessionVars("long").asInstanceOf[Long])
      }

      it("should accept an BigInteger") {
        CqlPreparedStatementUtil.asVarInt(defaultGatlingSession, "bigInteger") shouldBe a[BigInteger]
        CqlPreparedStatementUtil.asVarInt(defaultGatlingSession, "bigInteger") shouldBe
            defaultSessionVars("bigInteger").asInstanceOf[BigInteger]
      }

      it("should accept a string") {
        CqlPreparedStatementUtil.asVarInt(defaultGatlingSession, "intStr") shouldBe a[BigInteger]
        CqlPreparedStatementUtil.asVarInt(defaultGatlingSession, "intStr") shouldBe
            BigInteger.valueOf(defaultSessionVars("intStr").asInstanceOf[String].toInt)
      }

      it("should not accept a float and produce a CqlTypeException") {
        intercept[CqlTypeException] {
          CqlPreparedStatementUtil.asVarInt(defaultGatlingSession, "float") shouldBe a[BigInteger]
        }
      }

    }


    describe("asBigInt") {

      it("should accept an int") {
        CqlPreparedStatementUtil.asBigInt(defaultGatlingSession, "int") shouldBe a[java.lang.Long]
        CqlPreparedStatementUtil.asBigInt(defaultGatlingSession, "int") shouldBe
            defaultSessionVars("int").asInstanceOf[Int].asInstanceOf[Number].longValue()
      }

      it("should accept an long") {
        CqlPreparedStatementUtil.asBigInt(defaultGatlingSession, "long") shouldBe a[java.lang.Long]
        CqlPreparedStatementUtil.asBigInt(defaultGatlingSession, "long") shouldBe
            defaultSessionVars("long").asInstanceOf[Long].asInstanceOf[Number].longValue()
      }

      it("should accept a string") {
        CqlPreparedStatementUtil.asBigInt(defaultGatlingSession, "intStr") shouldBe a[java.lang.Long]
        CqlPreparedStatementUtil.asBigInt(defaultGatlingSession, "intStr") shouldBe
            defaultSessionVars("intStr").asInstanceOf[String].toInt.asInstanceOf[Number].longValue()
      }

      it("should accept a java.lang.number") {
        CqlPreparedStatementUtil.asBigInt(defaultGatlingSession, "javaNumber") shouldBe a[java.lang.Long]
        CqlPreparedStatementUtil.asBigInt(defaultGatlingSession, "javaNumber") shouldBe
            defaultSessionVars("javaNumber").asInstanceOf[Number].longValue()
      }

      it("should not accept a map and produce a CqlTypeException") {
        intercept[CqlTypeException] {
          CqlPreparedStatementUtil.asBigInt(defaultGatlingSession, "map") shouldBe a[java.lang.Long]
        }
      }

    }

    describe("asTimestamp") {

      it("should accept an epoch long") {
        CqlPreparedStatementUtil.asTimestamp(defaultGatlingSession, "epoch") shouldBe a[java.util.Date]
        CqlPreparedStatementUtil.asTimestamp(defaultGatlingSession, "epoch") shouldBe
            new java.util.Date(defaultSessionVars("epoch").asInstanceOf[Long])
      }

      it("should accept a java Date") {
        CqlPreparedStatementUtil.asTimestamp(defaultGatlingSession, "javaDate") shouldBe a[java.util.Date]
        CqlPreparedStatementUtil.asTimestamp(defaultGatlingSession, "javaDate") shouldBe
            defaultSessionVars("javaDate").asInstanceOf[java.util.Date]
      }

      it("should accept a date string") {
        CqlPreparedStatementUtil.asTimestamp(defaultGatlingSession, "dateString") shouldBe a[java.util.Date]
        CqlPreparedStatementUtil.asTimestamp(defaultGatlingSession, "dateString") shouldBe
            DateTime.parse(defaultSessionVars("dateString").asInstanceOf[String]).toDate
      }

      it("should accept a isoDateString string") {
        CqlPreparedStatementUtil.asTimestamp(defaultGatlingSession, "isoDateString") shouldBe a[java.util.Date]
        CqlPreparedStatementUtil.asTimestamp(defaultGatlingSession, "isoDateString") shouldBe
            DateTime.parse(defaultSessionVars("isoDateString").asInstanceOf[String]).toDate
      }

      it("should not accept a float and produce a CqlTypeException") {
        intercept[CqlTypeException] {
          CqlPreparedStatementUtil.asTimestamp(defaultGatlingSession, "float") shouldBe a[java.util.Date]
        }
      }

    }

    describe("asByte") {

      it("should accept a byteBuffer") {
        CqlPreparedStatementUtil.asByte(defaultGatlingSession, "byteBuffer") shouldBe a[ByteBuffer]
        CqlPreparedStatementUtil.asByte(defaultGatlingSession, "byteBuffer") shouldBe a[ByteBuffer]
      }

      it("should accept a byteArray") {
        CqlPreparedStatementUtil.asByte(defaultGatlingSession, "byteArray") shouldBe a[ByteBuffer]
        CqlPreparedStatementUtil.asByte(defaultGatlingSession, "byteArray") shouldBe a[ByteBuffer]
      }

      it("should accept a byte") {
        CqlPreparedStatementUtil.asByte(defaultGatlingSession, "byte") shouldBe a[ByteBuffer]
        CqlPreparedStatementUtil.asByte(defaultGatlingSession, "byte") shouldBe a[ByteBuffer]
      }

      it("should not accept a float and produce a CqlTypeException") {
        intercept[CqlTypeException] {
          CqlPreparedStatementUtil.asByte(defaultGatlingSession, "float") shouldBe a[ByteBuffer]
        }
      }
    }

    describe("asUdt") {

      val table = "udt_test"
      val typeName = "fullname"

      createType(keyspace, typeName, "firstname text, lastname text")
      createTable(keyspace, table, "id int, name frozen<fullname>, PRIMARY KEY(id)")

      val addressType = dseSession.getCluster.getMetadata.getKeyspace(keyspace).getUserType(typeName)

      val insertFullName = addressType.newValue()
          .setString("firstname", "John")
          .setString("lastname", "Smith")

      val newSessionVars = Map("fullname" -> insertFullName, "invalid" -> "string")

      val udtSession: Session = gatlingSession.setAll(newSessionVars)

      it("should accept a UDTValue") {
        CqlPreparedStatementUtil.asUdt(udtSession, "fullname") shouldBe a[UDTValue]
      }

      it("should not accept a float and produce a CqlTypeException") {
        intercept[CqlTypeException] {
          CqlPreparedStatementUtil.asUdt(udtSession, "invalid") shouldBe a[UDTValue]
        }
      }

    }

    describe("asTuple") {

      val table = "tuple_test"
      createTable(keyspace, table, "id int, tuple_type tuple<timestamp,varchar>, PRIMARY KEY (id)")

      val tupleType = dseSession.getCluster.getMetadata.newTupleType(DataType.varchar(), DataType.varchar())
      val insertTuple = tupleType.newValue("test", "test2")
      val newSessionVars = Map("tuple_type" -> insertTuple, "invalid" -> "string")
      val tupleSession: Session = gatlingSession.setAll(newSessionVars)

      it("should accept a TupleValue") {
        CqlPreparedStatementUtil.asTuple(tupleSession, "tuple_type") shouldBe a[TupleValue]
      }

      it("should not accept a float and produce a CqlTypeException") {
        intercept[CqlTypeException] {
          CqlPreparedStatementUtil.asTuple(tupleSession, "invalid") shouldBe a[TupleValue]
        }
      }

    }

  }


  describe("boundStatementFunctions") {


    val typeName = "fullname2"
    createType(keyspace, typeName, "firstname text, lastname text")

    val tableName = "type_table"
    createTable(keyspace, tableName, "uuid_type uuid, timeuuid_type timeuuid, int_type int, text_type text, " +
        "varchar_type varchar, ascii_type ascii, float_type float, double_type double, decimal_type decimal, " +
        "boolean_type boolean, inet_type inet, timestamp_type timestamp, bigint_type bigint, blob_type blob, " +
        "varint_type varint, list_type list<int>, set_type set<int>, map_type map<int,int>, date_type date, " +
        "smallint_type smallint, tinyint_type tinyint, time_type time, tuple_type tuple<varchar,varchar>, " +
        "udt_type frozen<fullname2>, null_type text, none_type text, PRIMARY KEY (uuid_type)")

    val counterTableName = "counter_type_table"
    createTable(keyspace, counterTableName, "uuid_type uuid, counter_type counter, PRIMARY KEY (uuid_type)")


    describe("bindParamByOrder") {

      val preparedStatementInsert =
        s"""INSERT INTO $keyspace.$tableName (
           |uuid_type,
           |timeuuid_type,
           |int_type,
           |text_type,
           |varchar_type,
           |ascii_type,
           |float_type,
           |double_type,
           |decimal_type,
           |boolean_type,
           |inet_type,
           |timestamp_type,
           |bigint_type,
           |blob_type,
           |varint_type,
           |list_type,
           |set_type,
           |map_type,
           |date_type,
           |smallint_type,
           |tinyint_type,
           |time_type,
           |tuple_type,
           |udt_type,
           |null_type,
           |none_type)
           |VALUES
           |(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""".stripMargin

      val boundStatementKeys = dseSession.prepare(preparedStatementInsert).bind()

      it("should bind with a UUID") {
        val result = CqlPreparedStatementUtil.bindParamByOrder(defaultGatlingSession, boundStatementKeys, DataType.Name.UUID, "uuid", 0)
        result shouldBe a[BoundStatement]
        result.isSet(0) shouldBe true
      }

      it("should bind with a timeUuid") {
        val result = CqlPreparedStatementUtil.bindParamByOrder(defaultGatlingSession, boundStatementKeys, DataType.Name.TIMEUUID, "timeUuid", 1)
        result shouldBe a[BoundStatement]
        result.isSet(1) shouldBe true
      }

      it("should bind with a int") {
        val result = CqlPreparedStatementUtil.bindParamByOrder(defaultGatlingSession, boundStatementKeys, DataType.Name.INT, "int", 2)
        result shouldBe a[BoundStatement]
        result.isSet(2) shouldBe true
      }

      it("should bind with a text") {
        val result = CqlPreparedStatementUtil.bindParamByOrder(defaultGatlingSession, boundStatementKeys, DataType.Name.TEXT, "string", 3)
        result shouldBe a[BoundStatement]
        result.isSet(3) shouldBe true
      }

      it("should bind with a varchar") {
        val result = CqlPreparedStatementUtil.bindParamByOrder(defaultGatlingSession, boundStatementKeys, DataType.Name.VARCHAR, "string", 4)
        result shouldBe a[BoundStatement]
        result.isSet(4) shouldBe true
      }

      it("should bind with a ascii") {
        val result = CqlPreparedStatementUtil.bindParamByOrder(defaultGatlingSession, boundStatementKeys, DataType.Name.ASCII, "string", 5)
        result shouldBe a[BoundStatement]
        result.isSet(5) shouldBe true
      }

      it("should bind with a float") {
        val result = CqlPreparedStatementUtil.bindParamByOrder(defaultGatlingSession, boundStatementKeys, DataType.Name.FLOAT, "float", 6)
        result shouldBe a[BoundStatement]
        result.isSet(6) shouldBe true
      }

      it("should bind with a double") {
        val result = CqlPreparedStatementUtil.bindParamByOrder(defaultGatlingSession, boundStatementKeys, DataType.Name.DOUBLE, "double", 7)
        result shouldBe a[BoundStatement]
        result.isSet(7) shouldBe true
      }

      it("should bind with a decimal") {
        val result = CqlPreparedStatementUtil.bindParamByOrder(defaultGatlingSession, boundStatementKeys, DataType.Name.DECIMAL, "double", 8)
        result shouldBe a[BoundStatement]
        result.isSet(8) shouldBe true
      }

      it("should bind with a boolean") {
        val result = CqlPreparedStatementUtil.bindParamByOrder(defaultGatlingSession, boundStatementKeys, DataType.Name.BOOLEAN, "boolean", 9)
        result shouldBe a[BoundStatement]
        result.isSet(9) shouldBe true
      }

      it("should bind with a inetAddress") {
        val result = CqlPreparedStatementUtil.bindParamByOrder(defaultGatlingSession, boundStatementKeys, DataType.Name.INET, "inetStr", 10)
        result shouldBe a[BoundStatement]
        result.isSet(10) shouldBe true
      }

      it("should bind with a timestamp") {
        val result = CqlPreparedStatementUtil.bindParamByOrder(defaultGatlingSession, boundStatementKeys, DataType.Name.TIMESTAMP, "epoch", 11)
        result shouldBe a[BoundStatement]
        result.isSet(11) shouldBe true
      }

      it("should bind with a bigInt") {
        val result = CqlPreparedStatementUtil.bindParamByOrder(defaultGatlingSession, boundStatementKeys, DataType.Name.BIGINT, "bigInteger", 12)
        result shouldBe a[BoundStatement]
        result.isSet(12) shouldBe true
      }

      it("should bind with a blob_type") {
        val result = CqlPreparedStatementUtil.bindParamByOrder(defaultGatlingSession, boundStatementKeys, DataType.Name.BLOB, "byteArray", 13)
        result shouldBe a[BoundStatement]
        result.isSet(13) shouldBe true
      }

      it("should bind with a varint") {
        val result = CqlPreparedStatementUtil.bindParamByOrder(defaultGatlingSession, boundStatementKeys, DataType.Name.VARINT, "int", 14)
        result shouldBe a[BoundStatement]
        result.isSet(14) shouldBe true
      }

      it("should bind with a list") {
        val result = CqlPreparedStatementUtil.bindParamByOrder(defaultGatlingSession, boundStatementKeys, DataType.Name.LIST, "list", 15)
        result shouldBe a[BoundStatement]
        result.isSet(15) shouldBe true
      }

      it("should bind with a set") {
        val result = CqlPreparedStatementUtil.bindParamByOrder(defaultGatlingSession, boundStatementKeys, DataType.Name.SET, "set", 16)
        result shouldBe a[BoundStatement]
        result.isSet(16) shouldBe true
      }

      it("should bind with a map") {
        val result = CqlPreparedStatementUtil.bindParamByOrder(defaultGatlingSession, boundStatementKeys, DataType.Name.MAP, "map", 17)
        result shouldBe a[BoundStatement]
        result.isSet(17) shouldBe true
      }

      it("should bind with a date") {
        val result = CqlPreparedStatementUtil.bindParamByOrder(defaultGatlingSession, boundStatementKeys, DataType.Name.DATE, "epoch", 18)
        result shouldBe a[BoundStatement]
        result.isSet(18) shouldBe true
      }

      it("should bind with a smallInt") {
        val result = CqlPreparedStatementUtil.bindParamByOrder(defaultGatlingSession, boundStatementKeys, DataType.Name.SMALLINT, "int", 19)
        result shouldBe a[BoundStatement]
        result.isSet(19) shouldBe true
      }

      it("should bind with a tinyint") {
        val result = CqlPreparedStatementUtil.bindParamByOrder(defaultGatlingSession, boundStatementKeys, DataType.Name.TINYINT, "int", 20)
        result shouldBe a[BoundStatement]
        result.isSet(20) shouldBe true
      }

      it("should bind with a time") {
        val result = CqlPreparedStatementUtil.bindParamByOrder(defaultGatlingSession, boundStatementKeys, DataType.Name.TIME, "epoch", 21)
        result shouldBe a[BoundStatement]
        result.isSet(21) shouldBe true
      }

      it("should bind with a tuple") {

        val tupleType = dseSession.getCluster.getMetadata.newTupleType(DataType.varchar(), DataType.varchar())
        val insertTuple = tupleType.newValue("test", "test2")
        val newSessionVars = Map("tuple_type" -> insertTuple, "invalid" -> "string")
        val tupleSession: Session = gatlingSession.setAll(newSessionVars)

        val result = CqlPreparedStatementUtil.bindParamByOrder(tupleSession, boundStatementKeys, DataType.Name.TUPLE, "tuple_type", 22)
        result shouldBe a[BoundStatement]

        result.isSet(22) shouldBe true
      }


      it("should bind with a udt") {

        val addressType = dseSession.getCluster.getMetadata.getKeyspace(keyspace).getUserType("fullname2")
        val insertFullName = addressType.newValue()
            .setString("firstname", "John")
            .setString("lastname", "Smith")
        val newSessionVars = Map("fullname2" -> insertFullName, "invalid" -> "string")
        val udtSession: Session = gatlingSession.setAll(newSessionVars)

        val result = CqlPreparedStatementUtil.bindParamByOrder(udtSession, boundStatementKeys, DataType.Name.UDT, "fullname2", 23)
        result shouldBe a[BoundStatement]
        result.isSet(23) shouldBe true
      }


      it("should bind with a counter") {

        val preparedStatementInsertCounter =
          s"""UPDATE $keyspace.$counterTableName SET counter_type = counter_type + ? WHERE uuid_type = ?"""

        val boundStatementCounter = dseSession.prepare(preparedStatementInsertCounter).bind()

        val result = CqlPreparedStatementUtil.bindParamByOrder(defaultGatlingSession, boundStatementCounter, DataType.Name.COUNTER, "int", 0)
        result shouldBe a[BoundStatement]
        result.isSet(0) shouldBe true
      }

      it("should bind with a null") {

        boundStatementKeys.isSet("null_type") shouldBe false

        val result = CqlPreparedStatementUtil.bindParamByOrder(defaultGatlingSession, boundStatementKeys, DataType.Name.TINYINT, "null_type", 24)
        result shouldBe a[BoundStatement]

        boundStatementKeys.isSet("null_type") shouldBe true
        boundStatementKeys.isNull("null_type") shouldBe true
      }

      it("should not set and unset a None value") {

        val field = "none_type"

        CqlPreparedStatementUtil.bindParamByOrder(defaultGatlingSession, boundStatementKeys, DataType.Name.TEXT, field, 25) shouldBe a[BoundStatement]
        boundStatementKeys.isSet(field) shouldBe false

        val newSessionVars = Map(field -> "test")
        val newSession: Session = gatlingSession.setAll(newSessionVars)

        CqlPreparedStatementUtil.bindParamByOrder(newSession, boundStatementKeys, DataType.Name.TEXT, field, 25) shouldBe a[BoundStatement]
        boundStatementKeys.isSet(field) shouldBe true
        boundStatementKeys.getString(field) shouldBe "test"

        CqlPreparedStatementUtil.bindParamByOrder(defaultGatlingSession, boundStatementKeys, DataType.Name.TEXT, field, 25) shouldBe a[BoundStatement]
        boundStatementKeys.isSet(field) shouldBe false
      }

      it("should not set a missing session value") {

        val field = "none_type"
        val newSessionVars = Map("missing" -> "test")
        val newSession: Session = gatlingSession.setAll(newSessionVars)

        CqlPreparedStatementUtil.bindParamByOrder(newSession, boundStatementKeys, DataType.Name.TEXT, field, 25) shouldBe a[BoundStatement]
        boundStatementKeys.isSet(field) shouldBe false
      }
    }


    describe("bindParamByName") {

      val preparedStatementInsert =
        s"""INSERT INTO $keyspace.$tableName (
           |uuid_type,
           |timeuuid_type,
           |int_type,
           |text_type,
           |varchar_type,
           |ascii_type,
           |float_type,
           |double_type,
           |decimal_type,
           |boolean_type,
           |inet_type,
           |timestamp_type,
           |bigint_type,
           |blob_type,
           |varint_type,
           |list_type,
           |set_type,
           |map_type,
           |date_type,
           |smallint_type,
           |tinyint_type,
           |time_type,
           |tuple_type,
           |udt_type,
           |null_type,
           |none_type)
           |VALUES (
           |:uuid_type,
           |:timeuuid_type,
           |:int_type,
           |:text_type,
           |:varchar_type,
           |:ascii_type,
           |:float_type,
           |:double_type,
           |:decimal_type,
           |:boolean_type,
           |:inet_type,
           |:timestamp_type,
           |:bigint_type,
           |:blob_type,
           |:varint_type,
           |:list_type,
           |:set_type,
           |:map_type,
           |:date_type,
           |:smallint_type,
           |:tinyint_type,
           |:time_type,
           |:tuple_type,
           |:udt_type,
           |:null_type,
           |:none_type
           |)""".stripMargin

      val boundStatementNames = dseSession.prepare(preparedStatementInsert).bind()

      val defaultSessionVars = Map(
        "uuid_type" -> java.util.UUID.randomUUID(),
        "timeuuid_type" -> UUIDs.timeBased(),
        "int_type" -> 12,
        "text_type" -> "string",
        "varchar_type" -> "string",
        "ascii_type" -> "string",
        "float_type" -> 12.0.toFloat,
        "double_type" -> 12.0,
        "decimal_type" -> 12.0,
        "boolean_type" -> true,
        "inet_type" -> InetAddress.getByName("127.0.0.1"),
        "timestamp_type" -> 1483299340813L,
        "bigint_type" -> 1483299340813L,
        "blob_type" -> Array(12.toByte),
        "varint_type" -> 12,
        "list_type" -> List(1),
        "set_type" -> Set(1),
        "map_type" -> Map(1 -> 1),
        "date_type" -> 1483299340813L,
        "smallint_type" -> 1,
        "tinyint_type" -> 1,
        "time_type" -> 1483299340813L,
        "counter_type" -> 1,
        "none_type" -> None,
        "null_type" -> null
      )

      val typeSession: Session = gatlingSession.setAll(defaultSessionVars)

      it("should bind with a UUID") {
        val paramName = "uuid_type"
        val result = CqlPreparedStatementUtil.bindParamByName(typeSession, boundStatementNames, DataType.Name.UUID, paramName)
        result shouldBe a[BoundStatement]
        boundStatementNames.isSet(paramName) shouldBe true
      }

      it("should bind with a timeUuid") {
        val paramName = "timeuuid_type"
        val result = CqlPreparedStatementUtil.bindParamByName(typeSession, boundStatementNames, DataType.Name.TIMEUUID, paramName)
        result shouldBe a[BoundStatement]
        boundStatementNames.isSet(paramName) shouldBe true
      }

      it("should bind with a int") {
        val paramName = "int_type"
        val result = CqlPreparedStatementUtil.bindParamByName(typeSession, boundStatementNames, DataType.Name.INT, paramName)
        result shouldBe a[BoundStatement]
        boundStatementNames.isSet(paramName) shouldBe true
      }

      it("should bind with a text") {
        val paramName = "text_type"
        val result = CqlPreparedStatementUtil.bindParamByName(typeSession, boundStatementNames, DataType.Name.TEXT, paramName)
        result shouldBe a[BoundStatement]
        boundStatementNames.isSet(paramName) shouldBe true
      }

      it("should bind with a varchar") {
        val paramName = "varchar_type"
        val result = CqlPreparedStatementUtil.bindParamByName(typeSession, boundStatementNames, DataType.Name.VARCHAR, paramName)
        result shouldBe a[BoundStatement]
        boundStatementNames.isSet(paramName) shouldBe true
      }

      it("should bind with a ascii") {
        val paramName = "ascii_type"
        val result = CqlPreparedStatementUtil.bindParamByName(typeSession, boundStatementNames, DataType.Name.ASCII, paramName)
        result shouldBe a[BoundStatement]
        boundStatementNames.isSet(paramName) shouldBe true
      }

      it("should bind with a float") {
        val paramName = "float_type"
        val result = CqlPreparedStatementUtil.bindParamByName(typeSession, boundStatementNames, DataType.Name.FLOAT, paramName)
        result shouldBe a[BoundStatement]
        boundStatementNames.isSet(paramName) shouldBe true
      }

      it("should bind with a double") {
        val paramName = "double_type"
        val result = CqlPreparedStatementUtil.bindParamByName(typeSession, boundStatementNames, DataType.Name.DOUBLE, paramName)
        result shouldBe a[BoundStatement]
        boundStatementNames.isSet(paramName) shouldBe true
      }

      it("should bind with a decimal") {
        val paramName = "decimal_type"
        val result = CqlPreparedStatementUtil.bindParamByName(typeSession, boundStatementNames, DataType.Name.DECIMAL, paramName)
        result shouldBe a[BoundStatement]
        boundStatementNames.isSet(paramName) shouldBe true
      }

      it("should bind with a boolean") {
        val paramName = "boolean_type"
        val result = CqlPreparedStatementUtil.bindParamByName(typeSession, boundStatementNames, DataType.Name.BOOLEAN, paramName)
        result shouldBe a[BoundStatement]
        boundStatementNames.isSet(paramName) shouldBe true
      }

      it("should bind with a inetAddress") {
        val paramName = "inet_type"
        val result = CqlPreparedStatementUtil.bindParamByName(typeSession, boundStatementNames, DataType.Name.INET, paramName)
        result shouldBe a[BoundStatement]
        boundStatementNames.isSet(paramName) shouldBe true
      }

      it("should bind with a timestamp") {
        val paramName = "timestamp_type"
        val result = CqlPreparedStatementUtil.bindParamByName(typeSession, boundStatementNames, DataType.Name.TIMESTAMP, paramName)
        result shouldBe a[BoundStatement]
        boundStatementNames.isSet(paramName) shouldBe true

      }

      it("should bind with a bigInt") {
        val paramName = "bigint_type"
        val result = CqlPreparedStatementUtil.bindParamByName(typeSession, boundStatementNames, DataType.Name.BIGINT, paramName)
        result shouldBe a[BoundStatement]
        boundStatementNames.isSet(paramName) shouldBe true
      }

      it("should bind with a blob_type") {
        val paramName = "blob_type"
        val result = CqlPreparedStatementUtil.bindParamByName(typeSession, boundStatementNames, DataType.Name.BLOB, paramName)
        result shouldBe a[BoundStatement]
        boundStatementNames.isSet(paramName) shouldBe true
      }

      it("should bind with a varint") {
        val paramName = "varint_type"
        val result = CqlPreparedStatementUtil.bindParamByName(typeSession, boundStatementNames, DataType.Name.VARINT, paramName)
        result shouldBe a[BoundStatement]
        boundStatementNames.isSet(paramName) shouldBe true
      }

      it("should bind with a list") {
        val paramName = "list_type"
        val result = CqlPreparedStatementUtil.bindParamByName(typeSession, boundStatementNames, DataType.Name.LIST, paramName)
        result shouldBe a[BoundStatement]
        boundStatementNames.isSet(paramName) shouldBe true
      }

      it("should bind with a set") {
        val paramName = "set_type"
        val result = CqlPreparedStatementUtil.bindParamByName(typeSession, boundStatementNames, DataType.Name.SET, paramName)
        result shouldBe a[BoundStatement]
        boundStatementNames.isSet(paramName) shouldBe true
      }

      it("should bind with a map") {
        val paramName = "map_type"
        val result = CqlPreparedStatementUtil.bindParamByName(typeSession, boundStatementNames, DataType.Name.MAP, paramName)
        result shouldBe a[BoundStatement]
        boundStatementNames.isSet(paramName) shouldBe true
      }

      it("should bind with a date") {
        val paramName = "date_type"
        val result = CqlPreparedStatementUtil.bindParamByName(typeSession, boundStatementNames, DataType.Name.DATE, paramName)
        result shouldBe a[BoundStatement]
        boundStatementNames.isSet(paramName) shouldBe true
      }

      it("should bind with a smallInt") {
        val paramName = "smallint_type"
        val result = CqlPreparedStatementUtil.bindParamByName(typeSession, boundStatementNames, DataType.Name.SMALLINT, paramName)
        result shouldBe a[BoundStatement]
        boundStatementNames.isSet(paramName) shouldBe true
      }

      it("should bind with a tinyint") {
        val paramName = "tinyint_type"
        val result = CqlPreparedStatementUtil.bindParamByName(typeSession, boundStatementNames, DataType.Name.TINYINT, paramName)
        result shouldBe a[BoundStatement]
        boundStatementNames.isSet(paramName) shouldBe true
      }

      it("should bind with a time") {
        val paramName = "time_type"
        val result = CqlPreparedStatementUtil.bindParamByName(typeSession, boundStatementNames, DataType.Name.TIME, paramName)
        result shouldBe a[BoundStatement]
        boundStatementNames.isSet(paramName) shouldBe true
      }

      it("should bind with a tuple") {
        val paramName = "tuple_type"
        val tupleType = dseSession.getCluster.getMetadata.newTupleType(DataType.varchar(), DataType.varchar())
        val insertTuple = tupleType.newValue("test", "test2")
        val newSessionVars = Map("tuple_type" -> insertTuple, "invalid" -> "string")
        val tupleSession: Session = gatlingSession.setAll(newSessionVars)

        val result = CqlPreparedStatementUtil.bindParamByName(tupleSession, boundStatementNames, DataType.Name.TUPLE, paramName)
        result shouldBe a[BoundStatement]
        boundStatementNames.isSet(paramName) shouldBe true
      }


      it("should bind with a udt") {
        val paramName = "udt_type"
        val addressType = dseSession.getCluster.getMetadata.getKeyspace(keyspace).getUserType("fullname2")
        val insertFullName = addressType.newValue()
            .setString("firstname", "John")
            .setString("lastname", "Smith")
        val newSessionVars = Map("udt_type" -> insertFullName, "invalid" -> "string")
        val udtSession: Session = gatlingSession.setAll(newSessionVars)

        val result = CqlPreparedStatementUtil.bindParamByName(udtSession, boundStatementNames, DataType.Name.UDT, paramName)
        result shouldBe a[BoundStatement]
        boundStatementNames.isSet(paramName) shouldBe true
      }


      it("should bind with a counter") {
        val paramName = "counter_type"
        val preparedStatementInsertCounter =
          s"""UPDATE $keyspace.$counterTableName SET counter_type = counter_type + :counter_type WHERE uuid_type = :uuid_type"""

        val boundNamedStatementCounter = dseSession.prepare(preparedStatementInsertCounter).bind()

        val result = CqlPreparedStatementUtil.bindParamByName(typeSession, boundNamedStatementCounter, DataType.Name.COUNTER, paramName)
        result shouldBe a[BoundStatement]
      }

      it("should bind with a null") {

        val paramName = "null_type"
        boundStatementNames.isSet(paramName) shouldBe false

        val result = CqlPreparedStatementUtil.bindParamByName(typeSession, boundStatementNames, DataType.Name.TINYINT, paramName)
        result shouldBe a[BoundStatement]

        boundStatementNames.isSet(paramName) shouldBe true
        boundStatementNames.isNull(paramName) shouldBe true
      }

      it("should not set and unset a None value") {

        val paramName = "none_type"

        val result = CqlPreparedStatementUtil.bindParamByName(typeSession, boundStatementNames, DataType.Name.TEXT, paramName)
        result shouldBe a[BoundStatement]
        boundStatementNames.isSet(paramName) shouldBe false

        val newSessionVars = Map(paramName -> "test")
        val newSession: Session = gatlingSession.setAll(newSessionVars)

        val result2 = CqlPreparedStatementUtil.bindParamByName(newSession, boundStatementNames, DataType.Name.TEXT, paramName)
        result2 shouldBe a[BoundStatement]
        boundStatementNames.isSet(paramName) shouldBe true
        boundStatementNames.getString(paramName) shouldBe "test"

        val result3 = CqlPreparedStatementUtil.bindParamByName(typeSession, boundStatementNames, DataType.Name.TEXT, paramName)
        result3 shouldBe a[BoundStatement]
        boundStatementNames.isSet(paramName) shouldBe false
      }

      it("should not set a missing session value") {

        val field = "none_type"
        val newSessionVars = Map("missing" -> "test")
        val newSession: Session = gatlingSession.setAll(newSessionVars)

        val result = CqlPreparedStatementUtil.bindParamByName(newSession, boundStatementNames, DataType.Name.TEXT, field)
        result shouldBe a[BoundStatement]
        boundStatementNames.isSet(field) shouldBe false
      }

    }
  }
}
