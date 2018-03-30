/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.exceptions

/**
  * Custom Exception to be thrown when type for boundStatement does not match accepted
  */
class DseCheckException(message: String = null, cause: Throwable = null) extends
    RuntimeException(DseCheckException.defaultMessage(message, cause), cause)

object DseCheckException {

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
