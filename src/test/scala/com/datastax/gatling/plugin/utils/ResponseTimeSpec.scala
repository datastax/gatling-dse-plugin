package com.datastax.gatling.plugin.utils

import java.util.concurrent.TimeUnit.{NANOSECONDS, SECONDS}

import com.datastax.gatling.plugin.base.BaseSimulationSpec
import io.gatling.core.session.Session
import org.easymock.EasyMock._


class ResponseTimeSpec extends BaseSimulationSpec {
  private val timingSource = mock[TimingSource]

  before {
    reset(timingSource)
  }
  describe("ResponseTime") {
    it("should compute the time elapsed since session start in milliseconds") {
      val session = Session("", 0, startDate = 123)
      expecting {
        timingSource.currentTimeNanos().andReturn(123456789)
      }
      whenExecuting(timingSource) {
        GatlingResponseTime(session, timingSource).latencyIn(NANOSECONDS) shouldBe 456789
      }
    }

    it("should delegate the start time to the session") {
      val session = Session("", 0, startDate = 44888)
      expecting {
        // Note that the latency is computed at `ResponseTime` creation
        // The mock must return something otherwise an NPE will be thrown
        timingSource.currentTimeNanos().andReturn(0)
      }
      whenExecuting(timingSource) {
        GatlingResponseTime(session, timingSource).startTimeIn(SECONDS) shouldBe 44
      }
    }

    it("should produce an instance of ResponseTimings for compatibility") {
      val session = Session("", 0, startDate = 111)
      expecting {
        timingSource.currentTimeNanos().andReturn(444666888)
      }
      whenExecuting(timingSource) {
        val rt = GatlingResponseTime(session, timingSource).toGatlingResponseTimings
        rt.startTimestamp shouldBe 111
        rt.endTimestamp shouldBe 444
      }
    }

    it("should rely on the timing source for service times as well") {
      expecting {
        timingSource.currentTimeNanos()
          .andReturn(4000000L) // 4ms
          .andReturn(5000000L) // 5ms
      }
      whenExecuting(timingSource) {
        val rt = COAffectedResponseTime.startingNow(timingSource).build().toGatlingResponseTimings
        rt.startTimestamp shouldBe 4
        rt.endTimestamp shouldBe 5
      }
    }
  }
}
