/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.metrics

import java.io.{FileOutputStream, PrintStream}
import java.nio.file.Paths
import java.util.Collections
import java.util.concurrent.{ConcurrentSkipListMap, TimeUnit}

import com.datastax.gatling.plugin.utils.ResponseTime
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.typesafe.scalalogging.LazyLogging
import io.gatling.core.Predef._
import io.gatling.core.session.Session
import org.HdrHistogram._

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.reflect.io.File

class HistogramLogger(startEpoch: Long) extends LazyLogging with MetricsLogger {
  protected val simName: String = configuration.core.simulationClass.get.split("\\.").last
  protected val histogramLogConfig: HistogramLogConfig = HistogramLogConfig.fromConfig()

  private final val baseDir: String = Paths.get(
    configuration.core.directory.results,
    MetricsLogger.sanitizeString(simName).toLowerCase + "-" + startEpoch,
    histogramLogConfig.directory).toString

  private val sanitizedGroups = new ConcurrentSkipListMap[String, String]()
  private val sanitizedTags = new ConcurrentSkipListMap[String, String]()
  private val tags = Collections.newSetFromMap(new ConcurrentSkipListMap[String, java.lang.Boolean]())
  private val groupTags = new ConcurrentSkipListMap[String, ArrayBuffer[String]]()

  private val globalHistogram = Map(
    "ok" -> new AtomicHistogram(histogramLogConfig.globalHighest, histogramLogConfig.globalRes),
    "ko" -> new AtomicHistogram(histogramLogConfig.globalHighest, histogramLogConfig.globalRes)
  )

  private val globalTagHistograms = Map(
    "ok" -> new ConcurrentSkipListMap[String, AtomicHistogram](),
    "ko" -> new ConcurrentSkipListMap[String, AtomicHistogram]()
  )

  private val globalGroupHistograms = Map(
    "ok" -> new ConcurrentSkipListMap[String, AtomicHistogram](),
    "ko" -> new ConcurrentSkipListMap[String, AtomicHistogram]()
  )

  private val perSecondTagHistograms = new
      ConcurrentSkipListMap[String, // For each tag name (request)
        ConcurrentSkipListMap[String, // And for each status (OK/KO)
          ConcurrentSkipListMap[Long, AtomicHistogram]]]() // And for each second, there is one histogram

  private val perSecondTagHistogramWriters = new
      ConcurrentSkipListMap[String, // For each tag name (request)
        ConcurrentSkipListMap[String, HistogramLogWriter]]() // And for each status (OK/KO), there is one file writer

  private val perSecondGroupHistograms = new
      ConcurrentSkipListMap[String, // For each group of requests
        ConcurrentSkipListMap[String, // And for each status (OK/KO)
          ConcurrentSkipListMap[Long, AtomicHistogram]]]() // And for each second, there is one histogram

  private val perSecondGroupHistogramWriters = new
      ConcurrentSkipListMap[String, // For each group of requests
        ConcurrentSkipListMap[String, HistogramLogWriter]]() // And for each status (OK/KO), there is one file writer

  /**
    * Log Metrics
    *
    * @param session        Gatling Session
    * @param tag            Event Tag
    * @param responseTime   Response Time
    * @param ok             OK/KO
    */
  def log(session: Session, tag: String, responseTime: ResponseTime, ok: Boolean): Unit = {
    val responseNanos = responseTime.latencyIn(TimeUnit.NANOSECONDS)

    if (System.currentTimeMillis() < (startEpoch + histogramLogConfig.logWriterWarmUp.toMillis)) {
      logger.trace(s"Current time is less than the warm up time ${histogramLogConfig.logWriterWarmUp}, " +
          s"skipping adding to histograms.")
      return
    }

    val groupId = getGroupId(session)
    val tagId = getTagId(groupId, tag)

    val status = if (ok) "ok" else "ko"

    // record to global all encompassing histogram
    globalHistogram(status).recordValue(responseNanos)

    // record into global histogram for group + tag
    globalTagHistograms(status)
        .computeIfAbsent(tagId, _ =>
          new AtomicHistogram(histogramLogConfig.globalHighest, histogramLogConfig.globalRes))
        .recordValue(responseNanos)

    // check if the tag has a histogram stored for the request time already
    perSecondTagHistograms
        .computeIfAbsent(tagId, _ => new ConcurrentSkipListMap())
        .computeIfAbsent(status, _ => new ConcurrentSkipListMap())
        .computeIfAbsent(responseTime.startTimeInSeconds, _ =>
          new AtomicHistogram(histogramLogConfig.globalHighest, histogramLogConfig.globalRes)
        ).recordValue(responseNanos)

    // log groups if enabled in the configs
    if (histogramLogConfig.groupLogsEnabled && groupId.nonEmpty) {

      globalGroupHistograms(status)
          .computeIfAbsent(groupId, _ => new AtomicHistogram(histogramLogConfig.globalHighest, histogramLogConfig.globalRes))
          .recordValue(responseNanos)

      perSecondGroupHistograms
          .computeIfAbsent(groupId, _ => new ConcurrentSkipListMap())
          .computeIfAbsent(status, _ => new ConcurrentSkipListMap())
          .computeIfAbsent(responseTime.startTimeInSeconds, _ =>
            new AtomicHistogram(histogramLogConfig.globalHighest, histogramLogConfig.globalRes)
          ).recordValue(responseNanos)
    }

  }

  /**
    * Close the File buffers
    */
  def close(): Unit = {
    val endTime = System.currentTimeMillis

    logger.info("Closing down HdrHistogram Metrics...")

    // flush global intervals
    globalHistogram.foreach { case (status: String, histogram: AtomicHistogram) =>
      initGlobalLogWriter(status).outputIntervalHistogram(startEpoch, endTime, histogram)
    }

    globalTagHistograms.foreach { case (status, tagHisto) =>
      tagHisto.keySet().forEach(tag =>
        initTagLogWriter(tag, status, "gbl").outputIntervalHistogram(startEpoch, endTime, tagHisto.get(tag))
      )
    }

    perSecondTagHistograms.keySet().asScala.foreach { tag =>
      perSecondTagHistograms.get(tag).keySet()
          .forEach(logTagHistogramsToFileUntil(tag, _, TimeUnit.MILLISECONDS.toSeconds(endTime) + 2))
    }

    perSecondGroupHistograms.keySet().asScala.foreach { group =>
      perSecondGroupHistograms.get(group).keySet()
          .forEach(logGroupHistogramsToFileUntil(group, _, TimeUnit.MILLISECONDS.toSeconds(endTime) + 2))
    }

    writeJsonFile(baseDir, endTime)
  }

  def writeNewData(): Unit = {
    perSecondTagHistograms.keySet().asScala.foreach { tag =>
      perSecondTagHistograms.get(tag).keySet().asScala.foreach(logIntervalTagHistogramsToFile(tag, _))
    }

    perSecondGroupHistograms.keySet().asScala.foreach { tag =>
      perSecondGroupHistograms.get(tag).keySet().asScala.foreach(logIntervalGroupHistogramsToFile(tag, _))
    }
  }

  /**
    * write the metrics captured details to a json file for reporting purposes
    *
    * @param baseDir base directory to store the hdrhistograms
    * @param endTime endtime of the simulation running
    */
  private def writeJsonFile(baseDir: String, endTime: Long): Unit = {
    val metricsJson = new MetricsJson(
      simName = simName,
      groups = groupTags,
      tags = tags,
      startTime = startEpoch,
      endTime = endTime)

    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.enable(SerializationFeature.INDENT_OUTPUT)
    objectMapper.writeValue(File(baseDir + File.separator + "metrics.json").outputStream(), metricsJson)
  }

  private def initGlobalLogWriter(status: String): HistogramLogWriter = {
    val globalHgrm = baseDir + File.separator + "Global_" + status + ".hgrm"

    File(baseDir).createDirectory()

    val log: PrintStream = new PrintStream(new FileOutputStream(globalHgrm), false)
    initHistogramLogWriter(log)
  }

  private def initTagLogWriter(tag: String, status: String, logType: String = "int"): HistogramLogWriter = {

    logger.trace("Base Dir: " + baseDir)
    val tagDir = baseDir + File.separator + "tags" + File.separator
    val tagFile = tagDir + tag + "_" + logType + "_" + status + ".hgrm"

    File(baseDir).createDirectory()
    File(tagDir).createDirectory()

    logger.trace("Creating hdr file: " + tagFile)
    val log: PrintStream = new PrintStream(new FileOutputStream(tagFile), false)

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

  private def initHistogramLogWriter(log: PrintStream) = {
    val histogramLogWriter: HistogramLogWriter = new HistogramLogWriter(log)
    histogramLogWriter.outputComment("[Logged with Gatling DSE Plugin v1.1.0]")
    histogramLogWriter.outputLogFormatVersion()
    histogramLogWriter.outputStartTime(startEpoch)
    histogramLogWriter.setBaseTime(startEpoch)
    histogramLogWriter.outputLegend()
    histogramLogWriter.get
  }

  private def logIntervalTagHistogramsToFile(tag: String, status: String) {
    logger.trace(s"Writing $tag:$status histograms older than ${histogramLogConfig.logWriterDelay.toSeconds} seconds ago")
    logTagHistogramsToFileUntil(tag, status,
      TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis) - histogramLogConfig.logWriterDelay.toSeconds)
  }

  private def logTagHistogramsToFileUntil(tag: String, status: String, maxTimestamp: Long) {
    logger.trace(s"Writing $tag:$status histograms created before $maxTimestamp")
    val perSecondHistograms = perSecondTagHistograms.get(tag).get(status)

    val writer = perSecondTagHistogramWriters
        .computeIfAbsent(tag, _ => new ConcurrentSkipListMap())
        .computeIfAbsent(status, initTagLogWriter(tag, _))

    var cnt = 0

    perSecondHistograms.headMap(maxTimestamp).forEach { (timestamp: Long, _: AtomicHistogram) =>
      writer.outputIntervalHistogram(timestamp, timestamp + 1, perSecondHistograms.remove(timestamp))
      cnt += 1
    }
    logger.debug(s"Logged $cnt seconds for $tag to histogram file")
  }

  private def logIntervalGroupHistogramsToFile(group: String, status: String) {
    logger.trace(s"Writing $group histograms older than ${histogramLogConfig.logWriterDelay.toSeconds} seconds ago")
    logGroupHistogramsToFileUntil(group, status,
      TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis) - histogramLogConfig.logWriterDelay.toSeconds)
  }

  private def logGroupHistogramsToFileUntil(group: String, status: String, maxTimestamp: Long) {
    logger.trace(s"Writing $group histograms created before $maxTimestamp")
    val perSecondHistograms = perSecondGroupHistograms.get(group).get(status)
    val writer = perSecondGroupHistogramWriters
        .computeIfAbsent(group, _ => new ConcurrentSkipListMap())
        .computeIfAbsent(status, initGroupLogWriter(group, _))
    var cnt = 0
    perSecondHistograms.headMap(maxTimestamp).forEach { (timestamp: Long, _) =>
      writer.outputIntervalHistogram(timestamp, timestamp + 1, perSecondHistograms.remove(timestamp))
      cnt += 1
    }
    logger.trace(s"Logged $cnt seconds for $group to histogram file")
  }

  private def getGroupId(session: Session): String = {
    if (session.groupHierarchy.isEmpty) {
      ""
    } else {
      sanitizedGroups.computeIfAbsent(session.groupHierarchy.mkString("_"), MetricsLogger.sanitizeString)
    }
  }

  private def getTagId(groupId: String, tag: String): String = {
    if (groupId.nonEmpty) {
      if (!groupTags.containsKey(groupId)) {
        groupTags.put(groupId, ArrayBuffer(tag))
      } else {
        if (!groupTags.get(groupId).contains(tag)) {
          groupTags.get(groupId).append(tag)
        }
      }
    }

    if (tag.nonEmpty && !tags.contains(tag)) {
      tags.add(tag)
    }

    sanitizedTags.computeIfAbsent(tag, MetricsLogger.sanitizeString)
  }
}

class MetricsJson(val simName: String,
                  val groups: java.util.Map[String, ArrayBuffer[String]],
                  val tags: java.util.Set[String],
                  val startTime: Long = 0,
                  val endTime: Long = 0)
