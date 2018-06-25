/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.metrics

import java.io.{Closeable, FileOutputStream, PrintStream}
import java.nio.file.{Path, Paths}
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.{ConcurrentMap, ConcurrentSkipListMap, TimeUnit}

import com.datastax.gatling.plugin.utils.ResponseTime
import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.Predef._
import io.gatling.core.session.Session
import org.HdrHistogram._

import scala.collection.JavaConverters._
import scala.reflect.io.File

class HistogramLogger(startEpoch: Long) extends StrictLogging with MetricsLogger {
  protected val simName: String = configuration.core.simulationClass.get.split("\\.").last
  protected val config: HistogramLogConfig = HistogramLogConfig.fromConfig()

  private final val baseDir: String = Paths.get(
    configuration.core.directory.results,
    MetricsLogger.sanitizeString(simName).toLowerCase + "-" + startEpoch,
    config.directory).toString

  /**
    * For each status (ok/ko) and at each second, there is one global histogram.
    */
  private val globalHistograms = new
      ConcurrentSkipListMap[String,
        ConcurrentSkipListMap[Long, AtomicHistogram]]()

  /**
    * For each status, there is one writer.
    */
  private val globalHistogramWriters = new
      ConcurrentSkipListMap[String, HistogramLogWriter]()

  /**
    * For each group of requests, for each status (ok/ko) and at each second,
    * there is one group histogram.
    */
  private val groupHistograms = new
      ConcurrentSkipListMap[String,
        ConcurrentSkipListMap[String,
          ConcurrentSkipListMap[Long, AtomicHistogram]]]()

  /**
    * For each group of requests and for each status, there is one writer.
    */
  private val groupHistogramWriters = new
      ConcurrentSkipListMap[String,
        ConcurrentSkipListMap[String, HistogramLogWriter]]()

  /**
    * For each tag name (request), for each status (ok/ko) and at each second,
    * there is one query histogram.
    */
  private val queryHistograms = new
      ConcurrentSkipListMap[String,
        ConcurrentSkipListMap[String,
          ConcurrentSkipListMap[Long, AtomicHistogram]]]()

  /**
    * For each tag name and for each status, there is one writer.
    */
  private val queryHistogramWriters = new
      ConcurrentSkipListMap[String,
        ConcurrentSkipListMap[String, HistogramLogWriter]]()

  private def newHistogram(config: HistogramCategoryConfig) =
    new AtomicHistogram(config.highestValue, config.resolution)

  /**
    * Log Metrics
    *
    * @param session      Gatling Session
    * @param tag          Event Tag
    * @param responseTime Response Time
    * @param ok           OK/KO
    */
  def log(session: Session, tag: String, responseTime: ResponseTime, ok: Boolean): Unit = {
    if (!config.enabled) {
      logger.debug("Histogram logger is disabled, nothing to do")
    } else if (System.currentTimeMillis() < (startEpoch + config.logWriterWarmUp.toMillis)) {
      logger.trace("Current time is less than the warm up time {}, skipping " +
        "adding to histograms.", config.logWriterWarmUp)
    } else {
      val groupId = session.groupHierarchy.map(MetricsLogger.sanitizeString).mkString("_")
      val tagId = MetricsLogger.sanitizeString(tag)
      val responseNanos = responseTime.latencyIn(TimeUnit.NANOSECONDS)
      val time = responseTime.startTimeInSeconds
      val status = if (ok) "ok" else "ko"

      if (config.globalHistograms.enabled) {
        globalHistograms
          .computeIfAbsent(status, _ => new ConcurrentSkipListMap())
          .computeIfAbsent(time, _ => newHistogram(config.globalHistograms))
          .recordValue(responseNanos)
      }

      if (config.groupHistograms.enabled && groupId.nonEmpty) {
        groupHistograms
          .computeIfAbsent(groupId, _ => new ConcurrentSkipListMap())
          .computeIfAbsent(status, _ => new ConcurrentSkipListMap())
          .computeIfAbsent(time, _ => newHistogram(config.groupHistograms))
          .recordValue(responseNanos)
      }

      if (config.queryHistograms.enabled) {
        groupHistograms
          .computeIfAbsent(tagId, _ => new ConcurrentSkipListMap())
          .computeIfAbsent(status, _ => new ConcurrentSkipListMap())
          .computeIfAbsent(time, _ => newHistogram(config.groupHistograms))
          .recordValue(responseNanos)
      }
    }
  }

  /**
    * Close the File buffers
    */
  def close(): Unit = {
    logger.info("Closing down HdrHistogram Metrics...")

    val endTimeStamp = System.currentTimeMillis
    writeDataUntil(endTimeStamp)
  }

  def writeNewData(): Unit = {
    val maxTimeStamp = MILLISECONDS.toSeconds(
      System.currentTimeMillis - config.logWriterDelay.toMillis)
    writeDataUntil(maxTimeStamp)
  }

  def writeDataUntil(maxTimeStamp: Long) {
    if (config.globalHistograms.enabled) {
      for {
        status <- globalHistograms.keySet().asScala
        histogramsToWrite <- globalHistograms.get(status).headMap(maxTimeStamp)
      } {
        logger.debug("Writing {} histograms {} created before {}",
          histogramsToWrite.size(), status, maxTimeStamp)
        val writer: HistogramLogWriter = globalHistogramWriters
          .computeIfAbsent(status, _ => initGlobalLogWriter(status))
        writeAndRemoveAll(histogramsToWrite, writer)
      }
    }

    if (config.groupHistograms.enabled) {
      for {
        group <- groupHistograms.keySet().asScala
        status <- groupHistograms.get(group).keySet().asScala
        histogramsToWrite <- groupHistograms.get(group).get(status).headMap(maxTimeStamp)
      } {
        logger.debug("Writing {} histograms {}:{} created before {}",
          histogramsToWrite.size(), group, status, maxTimeStamp)
        val writer = groupHistogramWriters
          .computeIfAbsent(group, _ => new ConcurrentSkipListMap())
          .computeIfAbsent(status, initGroupLogWriter(group, _))
        writeAndRemoveAll(histogramsToWrite, writer)
      }
    }

    if (config.queryHistograms.enabled) {
      for {
        tag <- queryHistograms.keySet().asScala
        status <- queryHistograms.get(tag).keySet().asScala
        histogramsToWrite <- queryHistograms.get(tag).get(status).headMap(maxTimeStamp)
      } {
        logger.debug("Writing {} histograms {}:{} created before {}",
          histogramsToWrite.size(), tag, status, maxTimeStamp)
        val writer = queryHistogramWriters
          .computeIfAbsent(tag, _ => new ConcurrentSkipListMap())
          .computeIfAbsent(status, initQueryLogWriter(tag, _))
        writeAndRemoveAll(histogramsToWrite, writer)
      }
    }
  }

  private def writeAndRemoveAll(histograms: ConcurrentMap[Long, AtomicHistogram],
                                writer: HistogramLogWriter): Unit =
    histograms
      .keySet()
      .forEach(t =>
        writer.outputIntervalHistogram(t, t + 1, histograms.remove(t)))

  private def initGlobalLogWriter(status: String): HistogramLogWriter = {
    val globalHgrm = baseDir + File.separator + "Global_" + status + ".hgrm"

    File(baseDir).createDirectory()

    val log: PrintStream = new PrintStream(new FileOutputStream(globalHgrm), false)
    initHistogramLogWriter(log)
  }

  private def initGroupLogWriter(group: String, status: String, logType: String = "int"): HistogramLogWriter = {

    logger.trace("Base Dir: " + baseDir)
    val groupDir = baseDir + File.separator + "groups" + File.separator
    val groupFile = groupDir + group + "_" + logType + "_" + status + ".hgrm"

    File(baseDir).createDirectory()
    File(groupDir).createDirectory()

    logger.trace("Creating hdr file: " + groupFile)
    val log: PrintStream = new PrintStream(new FileOutputStream(groupFile), false)

    initHistogramLogWriter(log)
  }

  private def initQueryLogWriter(tag: String, status: String, logType: String = "int"): HistogramLogWriter = {
    logger.trace("Base Dir: {}", baseDir)
    val tagDir = baseDir + File.separator + "tags" + File.separator
    val tagFile = tagDir + tag + "_" + logType + "_" + status + ".hgrm"
    File(baseDir).createDirectory()
    File(tagDir).createDirectory()

    logger.trace("Creating hdr file: {}", tagFile)
    val log: PrintStream = new PrintStream(new FileOutputStream(tagFile), false)
    initHistogramLogWriter(log)
  }

  private def initHistogramLogWriter(log: PrintStream) = {
    val histogramLogWriter: HistogramLogWriter = new HistogramLogWriter(log)
    histogramLogWriter.outputComment("[Logged with Gatling DSE Plugin v1.3.0]")
    histogramLogWriter.outputLogFormatVersion()
    histogramLogWriter.outputStartTime(startEpoch)
    histogramLogWriter.setBaseTime(startEpoch)
    histogramLogWriter.outputLegend()
    histogramLogWriter.get
  }
}


  }

class MetricsJson(val simName: String,
                  val groups: java.util.Map[String, ArrayBuffer[String]],
                  val tags: java.util.Set[String],
                  val startTime: Long = 0,
                  val endTime: Long = 0)
