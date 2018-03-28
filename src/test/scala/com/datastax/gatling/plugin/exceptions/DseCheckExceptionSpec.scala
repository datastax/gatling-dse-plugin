package com.datastax.gatling.plugin.exceptions

import com.datastax.gatling.plugin.base.BaseSpec

class DseCheckExceptionSpec extends BaseSpec {

  describe("DseCheckException") {

    it("should accept a message") {
      val thrown = intercept[DseCheckException] {
        throw new DseCheckException("I was thrown")
      }
      thrown.getMessage shouldBe "I was thrown"
    }

    it("should not require a message and use the cause") {
      val thrown = intercept[DseCheckException] {
        throw new DseCheckException(null, new Throwable("thrown"))
      }
      thrown.getMessage shouldBe new Throwable("thrown").getMessage
    }

    it("should not require a message or cause") {
      val thrown = intercept[DseCheckException] {
        throw new DseCheckException()
      }
      thrown.getMessage shouldBe null
    }

  }

}
