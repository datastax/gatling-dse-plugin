package com.datastax.gatling.plugin.exceptions

import com.datastax.gatling.plugin.base.BaseSpec

class CqlTypeExceptionSpec extends BaseSpec {

  describe("CqlTypeException") {

    it("should accept a message") {
      val thrown = intercept[CqlTypeException] {
        throw new CqlTypeException("I was thrown")
      }
      thrown.getMessage shouldBe "I was thrown"
    }

    it("should not require a message and use the cause") {
      val thrown = intercept[CqlTypeException] {
        throw new CqlTypeException(null, new Throwable("thrown"))
      }
      thrown.getMessage shouldBe new Throwable("thrown").getMessage
    }

    it("should not require a message or cause") {
      val thrown = intercept[CqlTypeException] {
        throw new CqlTypeException()
      }
      thrown.getMessage shouldBe null
    }

  }

}
