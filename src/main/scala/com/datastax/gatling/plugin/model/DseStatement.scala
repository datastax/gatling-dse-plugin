package com.datastax.gatling.plugin.model

import io.gatling.commons.validation.Validation
import io.gatling.core.session.Session

/**
  * This class serves as a common class between CQL and Graph queries.
  *
  * It contains a single method which allows a query (a instance of `Statement`
  * to be built from the Gatling session parameters, aka the feeders). A typical
  * usage is, for instance, finding the values to bind to CQL parameters.
  */
trait DseStatement[S] {
  def buildFromFeeders(session: Session): Validation[S]
}
