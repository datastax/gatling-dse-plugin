package com.datastax.gatling.plugin.exceptions

/**
  * Custom Exception to be thrown when type for boundStatement does not match accepted
  */
class DseCqlStatementException(message: String = null, cause: Throwable = null) extends
    RuntimeException(DseCqlStatementException.defaultMessage(message, cause), cause)

object DseCqlStatementException {

  def defaultMessage(message: String, cause: Throwable) = {
    if (message != null) {
      message
    } else if (cause != null) {
      cause.getMessage
    } else {
      null
    }
  }
}
