/*
 * Copyright (c) 2018 Datastax Inc.
 *
 * This software can be used solely with DataStax products. Please consult the file LICENSE.md.
 */

package com.datastax.gatling.plugin.checks

import com.datastax.dse.driver.api.core.graph.AsyncGraphResultSet
import com.datastax.oss.driver.api.core.cql.AsyncResultSet
import com.datastax.gatling.plugin.utils.ResultSetUtils

/**
  * Make both CQL and Graph checks available to the DSL.
  *
  * Note that as of 1.3.5 (and the upgrade to the unified OSS driver it brings along) the API here has changed.
  * The old check API exposed a rich set of checks for various operations including row counts and validating
  * data in individual rows.  Several (most?) of these checks were built on the idea that all rows were immediately
  * available in memory.  This design has changed in 1.3.5, so maintaining the old API would've proven quite difficult
  * (if not impossible).  Additionally, the rich API shields the user from the intricacies of the driver API at the
  * cost of limited flexibility; implementing new functionality requires modifications to the plugin itself (or at
  * least an awareness of it's innards).
  *
  * With 1.3.5 this relationship has been inverted.  The check API has been reduced to a single check which makes the
  * underlying [[AsyncResultSet]] or [[AsyncGraphResultSet]] available.  Simulations can then use transform() to
  * extract values and evaluate them as necessary.  So, for instance, something like this:
  *
  * {{{
  * .check(columnValue("counter_type") is 2)
  * }}}
  *
  * now becomes:
  *
  * {{{
  * .check(resultSet.transform(rs => rs.one().getLong("counter_type")) is 2L)
  * }}}
  *
  * Note that these transforms are now managed as Scala code within the simulations so they can be abstracted and
  * built into libraries which can be re-used across simulations.  Also note that this abstraction can be implemented
  * without modifying the plugin itself.
  *
  * A similar pattern applies to checks based on metadata.  So this:
  *
  * {{{
  * .check(exhausted is true)
  * }}}
  *
  * now becomes:
  *
  * {{{
  * .check(resultSet.transform(_.hasMorePages) is false)
  * .check(resultSet.transform(_.remaining) is 0)
  * }}}
  *
  * At this point we should also note that checks are now explicitly executed in the order in which hey are declared
  * in the simulation.  This matters because iterating through rows will impact methods such as remaining().  So, for
  * example, if you want to validate that a single row was returned and it contained a specific value you should do
  * something like:
  *
  * {{{
  * .check(resultSet.transform(_.remaining) is 1)
  * .check(resultSet.transform(rs => rs.one().getLong("counter_type")) is 2L)
  * }}}
  *
  * and not:
  *
  * {{{
  * .check(resultSet.transform(rs => rs.one().getLong("counter_type")) is 2L)
  * .check(resultSet.transform(_.remaining) is 1)
  * }}}
  *
  * Finally, in general the expectation is that you won't need to realize all rows in a result set, but if for some
  * reason you find this necessary this functionality is supplied in [[ResultSetUtils]].  This class also serves as
  * an example of the kind of abstraction over common extraction operations discussed above.
  */
trait DseCheckSupport {

  lazy val resultSet = CqlChecks.resultSet
  lazy val graphResultSet = GraphChecks.resultSet
}
