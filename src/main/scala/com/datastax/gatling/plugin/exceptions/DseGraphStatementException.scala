package com.datastax.gatling.plugin.exceptions

class DseGraphStatementException(message: String = null, cause: Throwable = null) extends
    RuntimeException(DseGraphStatementException.defaultMessage(message, cause), cause)

object DseGraphStatementException {

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
