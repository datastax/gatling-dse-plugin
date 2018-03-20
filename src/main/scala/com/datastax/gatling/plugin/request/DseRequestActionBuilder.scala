package com.datastax.gatling.plugin.request

import com.datastax.gatling.plugin.DseProtocol
import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.util.NameGen

class DseRequestActionBuilder(dseAttributes: DseAttributes) extends ActionBuilder with NameGen {

  def build(ctx: ScenarioContext, next: Action): Action = {
    val dseComponents = ctx.protocolComponentsRegistry.components(DseProtocol.DseProtocolKey)
    new DseRequestAction(
      dseAttributes.tag,
      next,
      ctx.system,
      ctx.coreComponents.statsEngine,
      dseComponents.dseProtocol,
      dseAttributes,
      dseComponents.metricsLogger,
      dseComponents.dseRequestsRouter)
  }
}
