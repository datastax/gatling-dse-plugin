package com.datastax.gatling.plugin.utils

import java.util.concurrent.TimeUnit.NANOSECONDS

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
        GatlingResponseTime(session, timingSource).startTimeInSeconds shouldBe 44
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
  }
}
