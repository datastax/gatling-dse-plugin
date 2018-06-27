/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.utils

import java.math.BigInteger
import java.net.InetAddress
import java.nio.ByteBuffer
import java.util
import java.util.Date
import java.util.concurrent.TimeUnit

import com.datastax.driver.core.DataType.Name._
import com.datastax.driver.core._
import com.datastax.driver.dse.geometry._
import com.datastax.driver.dse.geometry.codecs.PointCodec
import com.datastax.gatling.plugin.exceptions.CqlTypeException
import com.github.nscala_time.time.Imports.DateTime
import io.gatling.core.session.Session

import scala.collection.JavaConverters._
import scala.util.matching.Regex


trait CqlPreparedStatementUtil {

  protected val hourMinSecRegEx: Regex = """(\d+):(\d+):(\d+)""".r
  protected val hourMinSecNanoRegEx: Regex = """(\d+):(\d+):(\d+).(\d+{1,9})""".r

  def bindParamByOrder(gatlingSession: Session,
                       boundStatement: BoundStatement, paramType: DataType.Name,
                       paramName: String, key: Int): BoundStatement

  def bindParamByName(gatlingSession: Session, boundStatement: BoundStatement, paramType: DataType.Name,
                      paramName: String): BoundStatement

  def getParamsMap(preparedStatement: PreparedStatement): Map[String, DataType.Name]

  def getParamsList(preparedStatement: PreparedStatement): List[DataType.Name]
}

/**
  * Utilities for CQL Statement building
  */
object CqlPreparedStatementUtil extends CqlPreparedStatementUtil {


  /**
    * Bind CQL Prepared statement params by key order
    *
    * @param gatlingSession Gatling Session
    * @param boundStatement CQL BoundStatement
    * @param paramType      Type of param ie String, int, boolean
    * @param paramName      Gatling Session Attribute Name
    * @param key            Key/Order of param
    */
  def bindParamByOrder(gatlingSession: Session, boundStatement: BoundStatement, paramType: DataType.Name,
                       paramName: String, key: Int): BoundStatement = {

    if (!gatlingSession.attributes.contains(paramName)) {
      if (boundStatement.isSet(paramName)) {
        boundStatement.unset(paramName)
      }
      return boundStatement
    }

    gatlingSession.attributes.get(paramName) match {
      case Some(null) =>
        boundStatement.setToNull(paramName)
        boundStatement
      case Some(None) =>
        if (boundStatement.isSet(paramName)) {
          boundStatement.unset(paramName)
        }
        boundStatement
      case _ =>
        paramType match {
          case (VARCHAR | TEXT | ASCII) =>
            boundStatement.setString(key, asString(gatlingSession, paramName))
          case INT =>
            boundStatement.setInt(key, asInteger(gatlingSession, paramName))
          case BOOLEAN =>
            boundStatement.setBool(key, asBoolean(gatlingSession, paramName))
          case (UUID | TIMEUUID) =>
            boundStatement.setUUID(key, asUuid(gatlingSession, paramName))
          case FLOAT =>
            boundStatement.setFloat(key, asFloat(gatlingSession, paramName))
          case DOUBLE =>
            boundStatement.setDouble(key, asDouble(gatlingSession, paramName))
          case DECIMAL =>
            boundStatement.setDecimal(key, asDecimal(gatlingSession, paramName))
          case INET =>
            boundStatement.setInet(key, asInet(gatlingSession, paramName))
          case TIMESTAMP =>
            boundStatement.setTimestamp(key, asTimestamp(gatlingSession, paramName))
          case COUNTER =>
            boundStatement.setLong(key, asCounter(gatlingSession, paramName))
          case BIGINT =>
            boundStatement.setLong(key, asBigInt(gatlingSession, paramName))
          case BLOB =>
            boundStatement.setBytes(key, asByte(gatlingSession, paramName))
          case VARINT =>
            boundStatement.setVarint(key, asVarInt(gatlingSession, paramName))
          case LIST =>
            boundStatement.setList(key, asList(gatlingSession, paramName))
          case SET =>
            boundStatement.setSet(key, asSet(gatlingSession, paramName))
          case MAP =>
            boundStatement.setMap(key, asMap(gatlingSession, paramName))
          case UDT =>
            boundStatement.setUDTValue(key, asUdt(gatlingSession, paramName))
          case TUPLE =>
            boundStatement.setTupleValue(key, asTuple(gatlingSession, paramName))
          case DATE =>
            boundStatement.setDate(key, asDate(gatlingSession, paramName))
          case SMALLINT =>
            boundStatement.setShort(key, asSmallInt(gatlingSession, paramName))
          case TINYINT =>
            boundStatement.setByte(key, asTinyInt(gatlingSession, paramName))
          case TIME =>
            boundStatement.setTime(key, asTime(gatlingSession, paramName))
          case CUSTOM =>
            gatlingSession.attributes.get(paramName) match {
              case Some(p: Point) =>
                boundStatement.set(key, asPoint(gatlingSession, paramName), classOf[Point])
              case Some(p: LineString) =>
                boundStatement.set(key, asLineString(gatlingSession, paramName), classOf[LineString])
              case Some(p: Polygon) =>
                boundStatement.set(key, asPolygon(gatlingSession, paramName), classOf[Polygon])
              case _ =>
                throw new UnsupportedOperationException(s"$paramName on unknown CUSTOM type")
            }
          case unknown =>
            throw new UnsupportedOperationException(s"The data type specified $unknown is not yet supported")
        }
    }
  }

  /**
    * Bind CQL Prepared statement params by anem
    *
    * @param gatlingSession Gatling Session
    * @param boundStatement CQL BoundStatement
    * @param paramType      Type of param ie String, int, boolean
    * @param paramName      Gatling Session Attribute Value
    */
  def bindParamByName(gatlingSession: Session, boundStatement: BoundStatement, paramType: DataType.Name,
                      paramName: String): BoundStatement = {

    if (!gatlingSession.attributes.contains(paramName)) {
      if (boundStatement.isSet(paramName)) {
        boundStatement.unset(paramName)
      }
      return boundStatement
    }

    gatlingSession.attributes.get(paramName) match {
      case Some(null) =>
        boundStatement.setToNull(paramName)
        boundStatement
      case Some(None) =>
        if (boundStatement.isSet(paramName)) {
          boundStatement.unset(paramName)
        }
        boundStatement
      case _ =>
        paramType match {
          case (VARCHAR | TEXT | ASCII) =>
            boundStatement.setString(paramName, asString(gatlingSession, paramName))
          case INT =>
            boundStatement.setInt(paramName, asInteger(gatlingSession, paramName))
          case BOOLEAN =>
            boundStatement.setBool(paramName, asBoolean(gatlingSession, paramName))
          case (UUID | TIMEUUID) =>
            boundStatement.setUUID(paramName, asUuid(gatlingSession, paramName))
          case FLOAT =>
            boundStatement.setFloat(paramName, asFloat(gatlingSession, paramName))
          case DOUBLE =>
            boundStatement.setDouble(paramName, asDouble(gatlingSession, paramName))
          case DECIMAL =>
            boundStatement.setDecimal(paramName, asDecimal(gatlingSession, paramName))
          case INET =>
            boundStatement.setInet(paramName, asInet(gatlingSession, paramName))
          case TIMESTAMP =>
            boundStatement.setTimestamp(paramName, asTimestamp(gatlingSession, paramName))
          case BIGINT =>
            boundStatement.setLong(paramName, asBigInt(gatlingSession, paramName))
          case COUNTER =>
            boundStatement.setLong(paramName, asCounter(gatlingSession, paramName))
          case BLOB =>
            boundStatement.setBytes(paramName, asByte(gatlingSession, paramName))
          case VARINT =>
            boundStatement.setVarint(paramName, asVarInt(gatlingSession, paramName))
          case LIST =>
            boundStatement.setList(paramName, asList(gatlingSession, paramName))
          case SET =>
            boundStatement.setSet(paramName, asSet(gatlingSession, paramName))
          case MAP =>
            boundStatement.setMap(paramName, asMap(gatlingSession, paramName))
          case UDT =>
            boundStatement.setUDTValue(paramName, asUdt(gatlingSession, paramName))
          case TUPLE =>
            boundStatement.setTupleValue(paramName, asTuple(gatlingSession, paramName))
          case DATE =>
            boundStatement.setDate(paramName, asDate(gatlingSession, paramName))
          case SMALLINT =>
            boundStatement.setShort(paramName, asSmallInt(gatlingSession, paramName))
          case TINYINT =>
            boundStatement.setByte(paramName, asTinyInt(gatlingSession, paramName))
          case TIME =>
            boundStatement.setTime(paramName, asTime(gatlingSession, paramName))
          case CUSTOM =>
            gatlingSession.attributes.get(paramName) match {
              case Some(p: Point) =>
                boundStatement.set(paramName, asPoint(gatlingSession, paramName), classOf[Point])
              case Some(p: LineString) =>
                boundStatement.set(paramName, asLineString(gatlingSession, paramName), classOf[LineString])
              case Some(p: Polygon) =>
                boundStatement.set(paramName, asPolygon(gatlingSession, paramName), classOf[Polygon])
              case _ =>
                throw new UnsupportedOperationException(s"$paramName on unknown CUSTOM type")
            }
          case unknown =>
            throw new UnsupportedOperationException(s"The data type specified $unknown is not yet supported")
        }
    }
  }


  /**
    * Get Params in prepared statement in Map (name -> type)
    *
    * @param preparedStatement CQL Prepared Stated
    * @return
    */
  def getParamsMap(preparedStatement: PreparedStatement): Map[String, DataType.Name] = {
    val paramVariables = preparedStatement.getVariables
    val paramIterator = paramVariables.iterator.asScala
    paramIterator.map(p => (p.getName, p.getType.getName)).toMap
  }


  /**
    * Get Params in order of usage
    *
    * @param preparedStatement CQL Prepared Stated
    * @return
    */
  def getParamsList(preparedStatement: PreparedStatement): List[DataType.Name] = {
    val paramVariables = preparedStatement.getVariables
    paramVariables.iterator.asScala.map(p => p.getType.getName).toList
  }


  /**
    * Returns CQL compatible Integer type from one of the following session types:
    * - String
    *
    * @param gatlingSession Gatling Session
    * @param paramName      CQL prepared statement parameter name
    * @return
    */
  def asString(gatlingSession: Session, paramName: String): String = {
    gatlingSession.attributes.get(paramName).flatMap(Option(_)) match {
      case Some(s: String) =>
        s
      case _ =>
        throw new CqlTypeException(s"$paramName expected to be type of String")
    }
  }


  /**
    * Returns CQL compatible Integer type from one of the following session types:
    * - Int
    * - String
    *
    * @param gatlingSession Gatling Session
    * @param paramName      CQL prepared statement parameter name
    * @return
    */
  def asInteger(gatlingSession: Session, paramName: String): Integer = {
    gatlingSession.attributes.get(paramName).flatMap(Option(_)) match {
      case Some(i: Int) =>
        i
      case Some(s: String) =>
        s.toInt
      case _ =>
        throw new CqlTypeException(s"$paramName expected to be type of Int")
    }
  }


  /**
    * Returns CQL compatible Float type from one of the following session types:
    * - Float
    * - Double
    * - Int
    * - String
    *
    * @param gatlingSession Gatling Session
    * @param paramName      CQL prepared statement parameter name
    * @return
    */
  def asFloat(gatlingSession: Session, paramName: String): Float = {
    gatlingSession.attributes.get(paramName).flatMap(Option(_)) match {
      case Some(f: Float) =>
        f
      case Some(d: Double) =>
        d.toFloat
      case Some(i: Int) =>
        i.toFloat
      case Some(s: String) =>
        s.toFloat
      case _ =>
        throw new CqlTypeException(s"$paramName expected to be type of Float, Double or Int")
    }
  }


  /**
    * Returns CQL compatible UUID type from one of the following session types:
    * - Boolean: true / false
    * - String: "true" or "false"
    * - Int: < 1 = false, >= 1 = true
    *
    * @param gatlingSession Gatling Session
    * @param paramName      CQL prepared statement parameter name
    * @return
    */
  def asBoolean(gatlingSession: Session, paramName: String): Boolean = {
    gatlingSession.attributes.get(paramName).flatMap(Option(_)) match {
      case Some(b: Boolean) =>
        b
      case Some(s: String) =>
        if (s.equalsIgnoreCase("true")) {
          true
        } else if (s.equalsIgnoreCase("false")) {
          false
        } else {
          throw new CqlTypeException(s"$paramName expected to be string of true/false only")
        }
      case Some(i: Int) =>
        if (i < 1) {
          false
        } else {
          true
        }
      case _ =>
        throw new CqlTypeException(s"$paramName expected to be type of Boolean")
    }
  }


  /**
    * Returns CQL compatible UUID type from one of the following session types:
    * - java.util.UUID: native UUIDs
    * - String: runs java.util.UUID.fromString()
    *
    * @param gatlingSession Gatling Session
    * @param paramName      CQL prepared statement parameter name
    * @return
    */
  def asUuid(gatlingSession: Session, paramName: String): java.util.UUID = {
    gatlingSession.attributes.get(paramName).flatMap(Option(_)) match {
      case Some(uuid: java.util.UUID) =>
        uuid
      case Some(s: String) =>
        java.util.UUID.fromString(s)
      case _ =>
        throw new CqlTypeException(s"$paramName expected to be type of UUID")
    }
  }


  /**
    * Returns CQL compatible Double type from one of the following session types:
    * - Double
    * - Int
    *
    * @param gatlingSession Gatling Session
    * @param paramName      CQL prepared statement parameter name
    * @return
    */
  def asDouble(gatlingSession: Session, paramName: String): Double = {
    gatlingSession.attributes.get(paramName).flatMap(Option(_)) match {
      case Some(d: Double) =>
        d
      case Some(i: Int) =>
        i.toDouble
      case Some(s: String) =>
        s.toFloat.toDouble
      case _ =>
        throw new CqlTypeException(s"$paramName expected to be type of Double or Int")
    }
  }


  /**
    * Returns CQL compatible BigDecimal type from one of the following session types:
    * - Double
    * - Long
    * - java.math.BigDecimal
    * - String
    *
    * @param gatlingSession Gatling Session
    * @param paramName      CQL prepared statement parameter name
    * @return
    */
  def asDecimal(gatlingSession: Session, paramName: String): java.math.BigDecimal = {
    gatlingSession.attributes.get(paramName).flatMap(Option(_)) match {
      case Some(d: Double) =>
        BigDecimal(d).bigDecimal
      case Some(l: Long) =>
        BigDecimal(l).bigDecimal
      case Some(bg: java.math.BigDecimal) =>
        bg
      case Some(s: String) =>
        BigDecimal(s.toDouble).bigDecimal
      case _ =>
        throw new CqlTypeException(s"$paramName expected to be type of Double, Long or java.math.BigDecimal")
    }
  }


  /**
    * Returns CQL compatible Long type from one of the following session types:
    * - Long
    * - Int
    * - Number
    * - String
    *
    * @param gatlingSession Gatling Session
    * @param paramName      CQL prepared statement parameter name
    * @return
    */
  def asBigInt(gatlingSession: Session, paramName: String): Long = {
    gatlingSession.attributes.get(paramName).flatMap(Option(_)) match {
      case Some(l: Long) =>
        l.asInstanceOf[Number].longValue()
      case Some(i: Int) =>
        i.asInstanceOf[Number].longValue()
      case Some(n: Number) =>
        n.longValue()
      case Some(s: String) =>
        s.toLong.asInstanceOf[Number].longValue()
      case _ =>
        throw new CqlTypeException(s"$paramName expected to be type of Long, Int, String (int) or java.lang.Number")
    }
  }


  /**
    * Returns CQL compatible Long type from one of the following session types:
    * - Long
    * - Int
    *
    * @param gatlingSession Gatling Session
    * @param paramName      CQL prepared statement parameter name
    * @return
    */
  def asCounter(gatlingSession: Session, paramName: String): Long = {
    gatlingSession.attributes.get(paramName).flatMap(Option(_)) match {
      case Some(l: Long) =>
        l
      case Some(i: Int) =>
        i.toLong
      case _ =>
        throw new CqlTypeException(s"$paramName expected to be type of Long or Int")
    }
  }


  /**
    * Returns CQL compatible Date type from one of the following session types:
    * - Long
    * - String
    * - Date
    *
    * @param gatlingSession Gatling Session
    * @param paramName      CQL prepared statement parameter name
    * @return
    */
  def asTimestamp(gatlingSession: Session, paramName: String): java.util.Date = {
    gatlingSession.attributes.get(paramName).flatMap(Option(_)) match {
      case Some(l: Long) =>
        new Date(l)
      case Some(s: String) =>
        DateTime.parse(s).toDate
      case Some(d: Date) =>
        d
      case _ =>
        throw new CqlTypeException(s"$paramName expected to be type of Long, String or java.util.Date")
    }
  }


  /**
    * Returns CQL compatible Set type from one of the following session types:
    * - Scala: Set
    * - Java: util.Set
    *
    * @param gatlingSession Gatling Session
    * @param paramName      CQL prepared statement parameter name
    * @return
    */
  def asSet(gatlingSession: Session, paramName: String): util.Set[Any] = {
    gatlingSession.attributes.get(paramName).flatMap(Option(_)) match {
      case Some(m: Set[Any]@unchecked) =>
        m.asJava
      case Some(s: Seq[Any]@unchecked) =>
        s.toSet.asJava
      case Some(s: util.Set[Any]@unchecked) =>
        s
      case _ =>
        throw new CqlTypeException(s"$paramName expected to be type of Set")
    }
  }


  /**
    * Returns CQL compatible List type from one of the following session types:
    * - Scala: List
    * - Java: util.List
    *
    * @param gatlingSession Gatling Session
    * @param paramName      CQL prepared statement parameter name
    * @return
    */
  def asList(gatlingSession: Session, paramName: String): util.List[Any] = {
    gatlingSession.attributes.get(paramName).flatMap(Option(_)) match {
      case Some(m: List[Any]@unchecked) =>
        m.asJava
      case Some(s: Seq[Any]@unchecked) =>
        s.toList.asJava
      case Some(l: util.List[Any]@unchecked) =>
        l
      case _ =>
        throw new CqlTypeException(s"$paramName expected to be type of List")
    }
  }


  /**
    * Returns CQL compatible Map type from one of the following session types:
    * - Scala: Map
    * - Java: util.Map
    *
    * @param gatlingSession Gatling Session
    * @param paramName      CQL prepared statement parameter name
    * @return
    */
  def asMap(gatlingSession: Session, paramName: String): util.Map[Any, Any] = {
    gatlingSession.attributes.get(paramName).flatMap(Option(_)) match {
      case Some(m: Map[Any, Any]@unchecked) =>
        m.asJava
      case Some(mj: util.Map[Any, Any]@unchecked) =>
        mj
      case _ =>
        throw new CqlTypeException(s"$paramName expected to be type of Set")
    }
  }

  /**
    * Returns CQL compatible Point type
    *
    * @param gatlingSession Gatling Session
    * @param paramName      CQL prepared statement parameter name
    * @return
    */
  def asPoint(gatlingSession: Session, paramName: String): Point = {
    gatlingSession.attributes.get(paramName) match {
      case Some(p: Point) =>
        p
      case _ =>
        throw new CqlTypeException(s"$paramName expected to be type of Point")
    }
  }

  /**
    * Returns CQL compatible LineString type
    *
    * @param gatlingSession Gatling Session
    * @param paramName      CQL prepared statement parameter name
    * @return
    */
  def asLineString(gatlingSession: Session, paramName: String): LineString = {
    gatlingSession.attributes.get(paramName) match {
      case Some(p: LineString) =>
        p
      case _ =>
        throw new CqlTypeException(s"$paramName expected to be type of LineString")
    }
  }
  
  /**
    * Returns CQL compatible Polygon type
    *
    * @param gatlingSession Gatling Session
    * @param paramName      CQL prepared statement parameter name
    * @return
    */
  def asPolygon(gatlingSession: Session, paramName: String): Polygon = {
    gatlingSession.attributes.get(paramName) match {
      case Some(p: Polygon) =>
        p
      case _ =>
        throw new CqlTypeException(s"$paramName expected to be type of Polygon")
    }
  }

  /**
    * Returns CQL compatible Byte type from one of the following session types:
    * - Int
    * - Byte
    * - String
    *
    * @param gatlingSession Gatling Session
    * @param paramName      CQL prepared statement parameter name
    * @return
    */
  def asTinyInt(gatlingSession: Session, paramName: String): Byte = {
    gatlingSession.attributes.get(paramName).flatMap(Option(_)) match {
      case Some(i: Int) =>
        i.toByte
      case Some(b: Byte) =>
        b
      case Some(s: String) =>
        s.toInt.toByte
      case _ =>
        throw new CqlTypeException(s"$paramName expected to be type of Int or Byte")
    }
  }


  /**
    * Returns CQL compatible Short type from one of the following session types:
    * - Int
    * - Short
    * - String
    *
    * @param gatlingSession Gatling Session
    * @param paramName      CQL prepared statement parameter name
    * @return
    */
  def asSmallInt(gatlingSession: Session, paramName: String): Short = {
    gatlingSession.attributes.get(paramName).flatMap(Option(_)) match {
      case Some(i: Int) =>
        i.toShort
      case Some(s: Short) =>
        s
      case Some(s: String) =>
        s.toInt.toShort
      case _ =>
        throw new CqlTypeException(s"$paramName expected to be type of Int or Short")
    }
  }


  /**
    * Returns CQL compatible BigInteger type from one of the following session types:
    * - Int
    * - Long
    * - String
    * - java.math.BigInteger
    *
    * @param gatlingSession Gatling Session
    * @param paramName      CQL prepared statement parameter name
    * @return
    */
  def asVarInt(gatlingSession: Session, paramName: String): BigInteger = {
    gatlingSession.attributes.get(paramName).flatMap(Option(_)) match {
      case Some(i: Int) =>
        BigInteger.valueOf(i)
      case Some(l: Long) =>
        BigInteger.valueOf(l)
      case Some(s: String) =>
        BigInteger.valueOf(s.toLong)
      case Some(bi: BigInteger) =>
        bi
      case _ =>
        throw new CqlTypeException(s"$paramName expected to be type of Int, Long, String (int) or BigInteger")
    }
  }

  /**
    * Returns CQL compatible Long type from one of the following session types:
    * - Long: Nanoseconds since midnight today
    * - String: Time of data since midnight  i.e. 13:30:54.234
    *
    * @param gatlingSession Gatling Session
    * @param paramName      CQL prepared statement parameter name
    * @return
    */
  def asTime(gatlingSession: Session, paramName: String): Long = {
    gatlingSession.attributes.get(paramName).flatMap(Option(_)) match {
      case Some(l: Long) =>
        l
      case Some(s: String) =>
        s.trim match {
          case hourMinSecNanoRegEx(hour, min, second, nano) => parseTime(paramName, hour, min, second, nano)
          case hourMinSecRegEx(hour, min, second) => parseTime(paramName, hour, min, second, null)
          case _ => throw new CqlTypeException(s"$paramName not in format hh:mm:ss[.fffffffff]")
        }
      case _ =>
        throw new CqlTypeException(s"$paramName expected to be type of Long or String")
    }
  }

  private def parseTime(paramName: String, hourStr: String, minStr: String,
                        secStr: String, nanoStr: String = null) = {

    val hour = Integer.parseInt(hourStr)
    val min = Integer.parseInt(minStr)
    val sec = Integer.parseInt(secStr)
    val zeros = "000000000"

    var nanos = 0

    if (nanoStr != null) {
      if (nanoStr.length() > 9) {
        throw new CqlTypeException(s"$paramName not in format hh:mm:ss[.fffffffff]");
      }
      val nanos_s = nanoStr + zeros.substring(0, 9 - nanoStr.length())
      nanos = Integer.parseInt(nanos_s)
    }

    if (hour < 0 || hour >= 24) {
      throw new CqlTypeException(s"$paramName Hour out of bounds.")
    }

    if (min < 0 || min >= 60) {
      throw new CqlTypeException(s"$paramName Minute out of bounds.")
    }

    if (sec < 0 || sec >= 60) {
      throw new CqlTypeException(s"$paramName Seconds out of bounds.")
    }

    var rawTime: Long = 0
    rawTime += TimeUnit.HOURS.toNanos(hour)
    rawTime += TimeUnit.MINUTES.toNanos(min)
    rawTime += TimeUnit.SECONDS.toNanos(sec)
    rawTime += nanos

    rawTime
  }


  /**
    * Returns CQL compatible LocalDate object from one of the following session types:
    * - String: 2016-10-01
    * - Long: 1483299340813                 // millis since epoch
    * - Int: 45                             // days since epoch
    * - LocalDate: Native LocalDate
    *
    * @param gatlingSession Gatling Session
    * @param paramName      CQL prepared statement parameter name
    * @return
    */
  def asDate(gatlingSession: Session, paramName: String): com.datastax.driver.core.LocalDate = {
    gatlingSession.attributes.get(paramName).flatMap(Option(_)) match {
      case Some(s: String) =>
        val dateSplit = s.split("-").toList
        LocalDate.fromYearMonthDay(dateSplit.head.toInt, dateSplit(1).toInt, dateSplit(2).toInt)
      case Some(l: Long) =>
        LocalDate.fromMillisSinceEpoch(l)
      case Some(i: Int) =>
        LocalDate.fromDaysSinceEpoch(i)
      case Some(ld: com.datastax.driver.core.LocalDate) =>
        ld
      case _ =>
        throw new CqlTypeException(s"$paramName expected to be type of String, Long, Int or LocalDate")
    }
  }


  /**
    * Returns CQL compatible InetAddress type from one of the following session types:
    * - String
    * - InetAddress
    *
    * @param gatlingSession Gatling Session
    * @param paramName      CQL prepared statement parameter name
    * @return
    */
  def asInet(gatlingSession: Session, paramName: String): InetAddress = {
    gatlingSession.attributes.get(paramName).flatMap(Option(_)) match {
      case Some(string: String) =>
        InetAddress.getByName(string)
      case Some(inet: InetAddress) =>
        inet
      case _ =>
        throw new CqlTypeException(s"$paramName expected to be type of String or InetAddress")
    }
  }


  /**
    * Returns CQL compatible UDTValue type from one of the following session types:
    * - UDTValue
    *
    * @param gatlingSession Gatling Session
    * @param paramName      CQL prepared statement parameter name
    * @return
    */
  def asUdt(gatlingSession: Session, paramName: String): UDTValue = {
    gatlingSession.attributes.get(paramName).flatMap(Option(_)) match {
      case Some(udt: UDTValue) =>
        udt
      case _ =>
        throw new CqlTypeException(s"$paramName expected to be type of UDTValue")
    }
  }


  /**
    * Returns CQL compatible Tuple type from one of the following session types:
    * - TupleValue
    *
    * @param gatlingSession Gatling Session
    * @param paramName      CQL prepared statement parameter name
    * @return
    */
  def asTuple(gatlingSession: Session, paramName: String): TupleValue = {
    gatlingSession.attributes.get(paramName).flatMap(Option(_)) match {
      case Some(tuple: TupleValue) =>
        tuple
      case _ =>
        throw new CqlTypeException(s"$paramName expected to be type of TupleValue")
    }
  }


  /**
    * Returns CQL compatible ByteBuffer type from one of the following session types:
    * - ByteBuffer
    * - ByteArray (Array[Byte])
    * - Byte
    *
    * @param gatlingSession Gatling Session
    * @param paramName      CQL prepared statement parameter name
    * @return
    */
  def asByte(gatlingSession: Session, paramName: String): ByteBuffer = {
    gatlingSession.attributes.get(paramName).flatMap(Option(_)) match {
      case Some(bb: ByteBuffer) =>
        bb
      case Some(arr: Array[Byte]) =>
        ByteBuffer.wrap(arr)
      case Some(b: Byte) =>
        ByteBuffer.wrap(Array(b))
      case _ =>
        throw new CqlTypeException(s"$paramName expected to be type of ByteBuffer, Array[Byte] or Byte")
    }
  }
}
