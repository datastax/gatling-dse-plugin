/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.response

import com.datastax.gatling.plugin.model.{ DseCqlAttributes, DseGraphAttributes }
import com.datastax.oss.driver.api.core.cql._
import com.datastax.dse.driver.api.core.graph._
import com.typesafe.scalalogging.LazyLogging

class CqlResponse(cqlResultSet: AsyncResultSet, dseAttributes: DseCqlAttributes[_, _]) extends LazyLogging {
  def attributes:DseCqlAttributes[_,_] = dseAttributes
  def resultSet:AsyncResultSet = cqlResultSet
}

class GraphResponse(graphResultSet: AsyncGraphResultSet, dseAttributes: DseGraphAttributes[_,_]) extends LazyLogging {
  def attributes:DseGraphAttributes[_,_] = dseAttributes
  def resultSet:AsyncGraphResultSet = graphResultSet
}
