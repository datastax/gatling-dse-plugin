/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.checks

trait DseCheckSupport {

  lazy val resultSet = CqlChecks.resultSet
  lazy val graphResultSet = GraphChecks.resultSet
}

