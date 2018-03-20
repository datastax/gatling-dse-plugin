package com.datastax.gatling.plugin.metrics

import java.io.{FileOutputStream, PrintStream}
import java.util.concurrent.{ConcurrentSkipListMap, TimeUnit}

import akka.Done
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.datastax.gatling.plugin.utils.ResponseTimers
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.util.concurrent.FutureCallback
import com.typesafe.scalalogging.{LazyLogging, StrictLogging}
import io.gatling.core.Predef._
import io.gatling.core.session.Session
import org.HdrHistogram._

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, ExecutionException}
import scala.reflect.io.File
import scala.util.{Failure, Success, Try}

class HistogramLogger(actorSystem: ActorSystem, startEpoch: Long) extends MetricsLogger with LazyLogging {

  val resultsRecorder: ActorRef = actorSystem.actorOf(Props[DriverResultRecorder])

  implicit def asFiniteDuration(d: java.time.Duration): FiniteDuration = {
    scala.concurrent.duration.Duration.fromNanos(d.toNanos)
  }

  protected final val metricType = "hgrm"

  protected val histogramLogConfig: HistogramLogConfig = HistogramLogConfig(
    enabled = getMetricConf(metricType).getBoolean("enabled"),
    directory = getMetricConf(metricType).getString("directory"),
    logTypes = getMetricConf(metricType).getStringList("logTypes").asScala.toList,
    logWriterWarmUp = getMetricConf(metricType).getDuration("logWriter.warmUp"),
    logWriterDelay = getMetricConf(metricType).getDuration("logWriter.delay"),
    logWriterInterval = getMetricConf(metricType).getDuration("logWriter.interval"),
    globalHighest = getMetricConf(metricType).getDuration("globalHgrm.highestTrackableValue").toNanos,
    globalRes = getMetricConf(metricType).getInt("globalHgrm.resolution"),
    intervalHighest = getMetricConf(metricType).getDuration("intervalHgrm.highestTrackableValue"),
    intervalRes = getMetricConf(metricType).getInt("intervalHgrm.resolution"),
    groupLogsEnabled = getMetricConf(metricType).getBoolean("groupLogsEnabled")
  )

  private final val baseDir = getDirPath(
    configuration.core.directory.results,
    sanitizeString(simName).toLowerCase + "-" + startEpoch,
    histogramLogConfig.directory)

  private val globalHistogram = Map(
    "ok" -> new AtomicHistogram(histogramLogConfig.globalHighest, histogramLogConfig.globalRes),
    "ko" -> new AtomicHistogram(histogramLogConfig.globalHighest, histogramLogConfig.globalRes)
  )

  private val globalTagHistograms = Map(
    "ok" -> new ConcurrentSkipListMap[String, AtomicHistogram](),
    "ko" -> new ConcurrentSkipListMap[String, AtomicHistogram]()
  )

  private val perSecondTagHistograms = new ConcurrentSkipListMap[String,
      ConcurrentSkipListMap[String, ConcurrentSkipListMap[Long, AtomicHistogram]]]()

  private val perSecondTagHistogramWriters = new ConcurrentSkipListMap[String, ConcurrentSkipListMap[String, HistogramLogWriter]]()

  private val globalGroupHistograms = Map(
    "ok" -> new ConcurrentSkipListMap[String, AtomicHistogram](),
    "ko" -> new ConcurrentSkipListMap[String, AtomicHistogram]()
  )

  private val perSecondGroupHistograms = new ConcurrentSkipListMap[String,
      ConcurrentSkipListMap[String, ConcurrentSkipListMap[Long, AtomicHistogram]]]()

  private val perSecondGroupHistogramWriters = new ConcurrentSkipListMap[String, ConcurrentSkipListMap[String, HistogramLogWriter]]()

  private val scheduler = actorSystem.scheduler

  if (histogramLogConfig.enabled) {
    logger.info("HdrHistograms enabled, starting log flushing actor...")
    logger.debug(s"HdrHistograms flush writer " +
        s"delay: ${histogramLogConfig.logWriterDelay.toSeconds}s, " +
        s"interval: ${histogramLogConfig.logWriterInterval.toSeconds}s")

    val task = new Runnable {
      def run() {
        perSecondTagHistograms.keySet().asScala.foreach { tag =>
          perSecondTagHistograms.get(tag).keySet().asScala.foreach(logIntervalTagHistogramsToFile(tag, _))
        }

        perSecondGroupHistograms.keySet().asScala.foreach { tag =>
          perSecondGroupHistograms.get(tag).keySet().asScala.foreach(logIntervalGroupHistogramsToFile(tag, _))
        }
      }
    }

    implicit val executor: ExecutionContextExecutor = actorSystem.dispatcher

    scheduler.schedule(
      initialDelay = Duration(histogramLogConfig.logWriterDelay.toSeconds, TimeUnit.SECONDS),
      interval = Duration(histogramLogConfig.logWriterInterval.toSeconds, TimeUnit.SECONDS),
      runnable = task)

  } else {
    logger.debug("HDRHistogram logger is disabled, skipping initialization...")
  }

  /**
    * This method eventually logs the response passed as argument by putting it into a dedicated actor mailbox.
    * This is in order not to block the calling thread.
    */
  def logAsync[T](t: Try[T], c: FutureCallback[T]): Unit = resultsRecorder ! DriverResult(t, c)

  /**
    * Log Metrics
    *
    * @param session        Gatling Session
    * @param tag            Event Tag
    * @param responseTimers Response Timers
    * @param ok             OK/KO
    */
  def log(session: Session, tag: String, responseTimers: ResponseTimers, ok: Boolean): Unit = {
    if (!histogramLogConfig.enabled) {
      return
    }

    val responseNanos = responseTimers.diffNanos

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
        .computeIfAbsent(responseTimers.getStartTimeInSeconds, _ =>
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
          .computeIfAbsent(responseTimers.getStartTimeInSeconds, _ =>
            new AtomicHistogram(histogramLogConfig.globalHighest, histogramLogConfig.globalRes)
          ).recordValue(responseNanos)
    }

  }

  /**
    * Close the File buffers
    */
  def close(): Unit = {
    actorSystem.stop(resultsRecorder)

    if (!histogramLogConfig.enabled) {
      return
    }

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

  /**
    * write the metrics captured details to a json file for reporting purposes
    *
    * @param baseDir base directory to store the hdrhistograms
    * @param endTime endtime of the simulation running
    */
  private def writeJsonFile(baseDir: String, endTime: Long): Unit = {
    val metricsJson = MetricsJson(
      simName = simName,
      groups = groupTags,
      tags = tags,
      startTime = startEpoch,
      endTime = endTime
    )

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
}


case class HistogramLogConfig(enabled: Boolean = false,
                              directory: String = "hgrms",
                              logTypes: List[String] = List("hgrm"),
                              logWriterWarmUp: Duration = 500.milliseconds,
                              logWriterDelay: Duration = 2.seconds,
                              logWriterInterval: Duration = 1.seconds,
                              intervalHighest: Duration = 1.minute,
                              intervalRes: Int = 2,
                              globalHighest: Long = 1.minute.toNanos,
                              globalRes: Int = 2,
                              groupLogsEnabled: Boolean = false
                             )


case class MetricsJson(simName: String,
                       groups: java.util.Map[String, ArrayBuffer[String]],
                       tags: java.util.Set[String],
                       startTime: Long = 0,
                       endTime: Long = 0
                      )

case class DriverResult[T](t: Try[T], callback: FutureCallback[T])

class DriverResultRecorder extends Actor with StrictLogging {
  override def receive: Receive = {
    case DriverResult(t, callback) => t match {
      case Success(resultSet) => callback.onSuccess(resultSet)
      case Failure(exception: ExecutionException) => callback.onFailure(exception.getCause)
      case Failure(exception: Exception) => callback.onFailure(exception)
      case Failure(exception: Throwable) =>
        logger.error("Caught an unexpected error, please file a ticket", exception)
        callback.onFailure(exception)
    }
  }
}
