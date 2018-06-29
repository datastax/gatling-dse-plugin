package com.datastax.gatling.plugin.request

import com.datastax.gatling.plugin.DseProtocol
import com.datastax.gatling.plugin.model.DseGraphAttributes
import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.structure.ScenarioContext
import io.gatling.core.util.NameGen

class GraphRequestActionBuilder(dseAttributes: DseGraphAttributes) extends ActionBuilder with
  NameGen {

  def build(ctx: ScenarioContext, next: Action): Action = {
    val dseComponents = ctx.protocolComponentsRegistry.components(DseProtocol.DseProtocolKey)
    new GraphRequestAction(
      dseAttributes.tag,
      next,
      ctx.system,
      ctx.coreComponents.statsEngine,
      dseComponents.dseProtocol,
      dseAttributes,
      dseComponents.metricsLogger,
      dseComponents.dseExecutorService,
      dseComponents.gatlingTimingSource)
  }
}
