/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.checks

import com.datastax.gatling.plugin.response.DseResponse
import io.gatling.commons.validation.SuccessWrapper
import io.gatling.core.check._

object DseCheckBuilders {

  private def responseExtender(): Extender[DseCheck, DseResponse] = {
    (wrapped: Check[DseResponse]) => DseCheck(wrapped)
  }

  val ResponseExtender: Extender[DseCheck, DseResponse] = responseExtender()

  val PassThroughResponsePreparer: Preparer[DseResponse, DseResponse] = (r: DseResponse) => r.success
}
