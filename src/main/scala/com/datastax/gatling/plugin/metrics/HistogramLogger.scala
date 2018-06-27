/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.metrics

import java.io.{Closeable, FileOutputStream, PrintStream}
import java.nio.file.{Path, Paths}
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.TimeUnit.{MILLISECONDS, NANOSECONDS}

import com.datastax.gatling.plugin.utils.ResponseTime
import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.Predef._
import io.gatling.core.session.Session
import org.HdrHistogram._

import scala.collection.JavaConversions._

class HistogramLogger(startTimeMillis: Long) extends StrictLogging with MetricsLogger {
  protected val simName: String = configuration.core.simulationClass.get.split("\\.").last
  protected val config: HistogramLogConfig = HistogramLogConfig.fromConfig()

  private final val baseDir: String = Paths.get(
    configuration.core.directory.results,
    MetricsLogger.sanitizeString(simName).toLowerCase + "-" + startTimeMillis,
    config.directory).toString

  /**
    * For each status (ok/ko) and at each second, there is one global histogram.
    */
  private val globalHistograms = new
      ConcurrentSkipListMap[String, PerSecondHistogram]()

  /**
    * For each group of requests, for each status (ok/ko) and at each second,
    * there is one group histogram.
    */
  private val groupHistograms = new
      ConcurrentSkipListMap[String,
        ConcurrentSkipListMap[String, PerSecondHistogram]]()

  /**
    * For each tag name (request), for each status (ok/ko) and at each second,
    * there is one query histogram.
    */
  private val queryHistograms = new
      ConcurrentSkipListMap[String,
        ConcurrentSkipListMap[String, PerSecondHistogram]]()

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
    } else if (System.currentTimeMillis() < (startTimeMillis + config.logWriterWarmUp.toMillis)) {
      logger.debug("Current time is less than the warm up time {}, skipping " +
        "adding to histograms.", config.logWriterWarmUp)
    } else {
      logger.debug("Recording latency for {}-{}: {}ns", tag, ok, responseTime.latencyIn(NANOSECONDS))
      val groupId = session.groupHierarchy.map(MetricsLogger.sanitizeString).mkString("_")
      val tagId = MetricsLogger.sanitizeString(tag)
      val responseNanos = responseTime.latencyIn(NANOSECONDS)
      val requestTime: Long = responseTime.startTimeInSeconds
      val status = if (ok) "ok" else "ko"

      if (config.globalHistograms.enabled) {
        val hgrmFile = Paths.get(baseDir, s"Global_${status}.hgrm")
        logger.debug("Recording in global histogram {}", hgrmFile)
        globalHistograms
          .computeIfAbsent(status, _ => new PerSecondHistogram(hgrmFile, requestTime, config.globalHistograms))
          .recordLatency(requestTime, responseNanos)
      }

      if (config.groupHistograms.enabled) {
        if (groupId.isEmpty) {
          logger.warn("Group level results are enabled but no group was found for query {}", tagId)
        } else {
          val hgrmFile = Paths.get(baseDir, "groups", s"${groupId}_int_$status.hgrm")
          logger.debug("Recording in group histogram {}", hgrmFile)
          groupHistograms
            .computeIfAbsent(groupId, _ => new ConcurrentSkipListMap())
            .computeIfAbsent(status, _ => new PerSecondHistogram(hgrmFile, requestTime, config.groupHistograms))
            .recordLatency(requestTime, responseNanos)
        }
      }

      if (config.queryHistograms.enabled) {
        val hgrmFile = Paths.get(baseDir, "tags", s"${tagId}_int_$status.hgrm")
        logger.debug("Recording in query histogram {}", hgrmFile)
        queryHistograms
          .computeIfAbsent(tagId, _ => new ConcurrentSkipListMap())
          .computeIfAbsent(status, _ => new PerSecondHistogram(hgrmFile, requestTime, config.queryHistograms))
          .recordLatency(requestTime, responseNanos)
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
    logger.debug("Writing data created until {}", maxTimeStamp)
    if (config.globalHistograms.enabled) {
      logger.debug("Writing global histograms that contains keys {}", globalHistograms.keySet())
      for {
        status <- globalHistograms.keySet()
      } {
        logger.debug("Writing global {} histograms", status)
        globalHistograms.get(status).writeUntil(maxTimeStamp)
      }
    }

    if (config.groupHistograms.enabled) {
      logger.debug("Writing group histograms that contains keys {}",
        groupHistograms.flatMap { case (tag, statusHistograms) =>
          statusHistograms.keySet().map(status => tag + "-" + status)
        }.toList)
      for {
        group <- groupHistograms.keySet()
        status <- groupHistograms.get(group).keySet()
      } {
        logger.debug("Writing group {}:{} histograms", group, status)
        groupHistograms.get(group).get(status).writeUntil(maxTimeStamp)
      }
    }

    if (config.queryHistograms.enabled) {
      logger.debug("Writing query histograms that contains keys {}",
        queryHistograms.flatMap { case (tag, statusHistograms) =>
          statusHistograms.keySet().map(status => tag + "-" + status)
        }.toList)
      for {
        tag <- queryHistograms.keySet()
        status <- queryHistograms.get(tag).keySet()
      } {
        logger.debug("Writing query {}:{} histograms", tag, status)
        queryHistograms.get(tag).get(status).writeUntil(maxTimeStamp)
      }
    }
  }
}

class PerSecondHistogram(val hgrmPath: Path,
                         val startTimeMillis: Long,
                         val config: HistogramCategoryConfig)
  extends StrictLogging with Closeable {
  /**
    * For each second, there is one histogram.
    */
  private val histograms = new ConcurrentSkipListMap[Long, AtomicHistogram]()

  /**
    * There is a single writer for this histogram.
    */
  private lazy val writer: HistogramLogWriter = initWriter()

  private def initWriter() = {
    logger.debug("Creating histogram writer for {}", hgrmPath)
    hgrmPath.getParent.toFile.mkdirs()
    val result = new HistogramLogWriter(
      new PrintStream(
        new FileOutputStream(hgrmPath.toString)))
    result.outputComment("[Logged with Gatling DSE Plugin v1.3.0]")
    result.outputLogFormatVersion()
    result.outputStartTime(startTimeMillis)
    result.setBaseTime(startTimeMillis)
    result.outputLegend()
    result
  }

  override def close(): Unit =
    writer.close()

  def recordLatency(timeSec: Long, latencyNanos: Long): Unit =
    histograms
      .computeIfAbsent(timeSec, _ => newHistogram())
      .recordValue(latencyNanos)

  def writeUntil(maxTimeStampSec: Long) {
    val histogramsToWrite = histograms.headMap(maxTimeStampSec)
    logger.debug("Writing {} histograms created before {}",
      histogramsToWrite.size(), maxTimeStampSec)
    histogramsToWrite
      .keySet()
      .forEach(t =>
        writer.outputIntervalHistogram(t, t + 1, histograms.remove(t)))
  }

  private def newHistogram() =
    new AtomicHistogram(config.highestValue, config.resolution)

}
