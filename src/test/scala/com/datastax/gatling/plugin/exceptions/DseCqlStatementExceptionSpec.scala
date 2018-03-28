package com.datastax.gatling.plugin.exceptions

import com.datastax.gatling.plugin.base.BaseSpec


class DseCqlStatementExceptionSpec extends BaseSpec {

  describe("DseCqlStatementException") {

    it("should accept a message") {
      val thrown = intercept[DseCqlStatementException] {
        throw new DseCqlStatementException("I was thrown")
      }
      thrown.getMessage shouldBe "I was thrown"
    }

    it("should not require a message and use the cause") {
      val thrown = intercept[DseCqlStatementException] {
        throw new DseCqlStatementException(null, new Throwable("thrown"))
      }
      thrown.getMessage shouldBe new Throwable("thrown").getMessage
    }

    it("should not require a message or cause") {
      val thrown = intercept[DseCqlStatementException] {
        throw new DseCqlStatementException()
      }
      thrown.getMessage shouldBe null
    }

  }

}
