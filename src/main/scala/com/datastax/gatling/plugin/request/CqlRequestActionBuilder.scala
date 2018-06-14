/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.request

import com.datastax.gatling.plugin.DseProtocol
import com.datastax.gatling.plugin.model.DseCqlAttributes
import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.util.NameGen

class CqlRequestActionBuilder(val dseAttributes: DseCqlAttributes) extends ActionBuilder with
  NameGen {

  def build(ctx: ScenarioContext, next: Action): Action = {
    val dseComponents = ctx.protocolComponentsRegistry.components(DseProtocol.DseProtocolKey)
    new CqlRequestAction(
      dseAttributes.tag,
      next,
      ctx.system,
      ctx.coreComponents.statsEngine,
      dseComponents.dseProtocol,
      dseAttributes,
      dseComponents.metricsLogger,
      dseComponents.dseRequestsRouter,
      dseComponents.gatlingTimingSource)
  }
}

