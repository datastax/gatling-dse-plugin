package com.datastax.gatling.plugin.utils

import java.lang.System.{currentTimeMillis, nanoTime}

import com.datastax.gatling.plugin.base.BaseSimulationSpec
import com.typesafe.config.ConfigFactory
import io.gatling.core.stats.message.ResponseTimings


class ResponseTimersSpec extends BaseSimulationSpec {

  val conf = ConfigFactory.load
  val currentTimes = (nanoTime, currentTimeMillis)
  val responseTimers = ResponseTimers(currentTimes)

  describe("Defaults") {

    it("should accept a tuple of Long values") {
      responseTimers.responseTimings.startTimestamp shouldBe currentTimes._2
      responseTimers.startTimes shouldBe currentTimes
    }

    it("should return in ms if set to native report format") {
      Thread sleep 500
      val endTime = responseTimers.responseTimings.endTimestamp
      responseTimers.responseTimings.startTimestamp shouldBe currentTimes._2
      endTime should (be > responseTimers.responseTimings.startTimestamp)
      endTime.toString.length shouldBe 13
    }
  }

  describe("responseTimings") {

    it("should be an instance of io.gatling.core.stats.message.ResponseTimings") {
      val responseTimings = responseTimers.responseTimings
      responseTimings shouldBe a[ResponseTimings]
    }
  }

  describe("getNanosAndMs") {

    it("should return a tuple") {
      val getNanosAndMs = responseTimers.getNanosAndMs
      getNanosAndMs shouldBe a[(_, _)]
    }
  }

  describe("getStartTimeInSeconds") {

    it("should return a long") {
      val getStartTimeInSeconds = responseTimers.getStartTimeInSeconds
      getStartTimeInSeconds shouldBe a[java.lang.Long]
    }
  }

}
