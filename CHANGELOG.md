# Change Log

## [Unreleased]

## [v1.1.1] - Palladium Panther - 2017-11-08
New:
- Updated to latest DSE and Graph Driver (v1.4.0)
- Added GraphFluentSessionKey exec method to allow a Graph Fluent API Traversal to be stored in a session and used
- Added Gradle publishing
- Updating dependency libraries

Fixes:
- Enhanced logs to include both the Group(s) and the tag


## [v1.1.0] - Iridium Iguana - 2017-10-06
New:
- Updated to latest DSE and Graph Driver (v1.4.0)
- Rewrite of core classes merging Graph and CQL classes into unified DSE classes
- Added support for CQL request w/ Custom Payloads and SimpleStatements
- Added support to set all available DSE Driver request params including:
  - CQL and Graph: idempotency, defaultTimestamp, readTimeout, and proxy user/role
  - CQL: retry policy, fetchSize, paging size, paging state, and tracing
  - Graph: read/write consistency, language, source, name, internalOptions, systemQuery and transform results function
- Added additional check() parameters including:
    - CQL and Graph: executionInfo, achievedCL, queriedHost, triedHosts, dseAttributes sent during request and more
    - CQL: resultSet, allRows, oneRow
    - Graph: graphResultSet, allNodes, oneNode, and edges, vertexes, paths, properties, and vertexProperties for a column
- Added .withSessionParams() to executePrepared() matching excuteNamed() functionality
- Enhanced logging for request errors adding host tried and query being used
- Addition of HDRHistogram usage for metrics at nanosecond granularity as an alternative to Gatling's simulation logs
- Reduction of thread creation for each request response processing

Breaking Changes:
- Scala 2.12 is required to compile
- Gatling 2.3+ is required to compile

## [v1.0.2] - Terbium Tiger - 2017-06-22
New:
- Added support for executing prepared batch statements
- Added support for setting retryPolicy, readTimeout, fetchSize, defaultTimestamp and idempotency driver options on each query


## [v1.0.1] - Mercury Meerkat - 2017-06-19
New:
- Updated to latest DSE Driver and Graph Driver (v1.2.4)
- Re-architect how 'null' and 'None' values in session/feeds are bound to CQL requests
  - `null` = null sent to DSE Cassandra (i.e. column tombstone)
  - `None` = ignore or unset if already set
  - missing param = ignore


## [v1.0.0] - Copper Fox - 2017-05-14
New:
- Updated to latest DSE Driver and Graph Driver (v1.2.3)
- Added [proxy auth](https://docs.datastax.com/en/developer/java-driver-dse/1.2/manual/auth/) support for CQL queries (by [@pingtimeout](https://github.com/pingtimeout))
- Added microsecond and millisecond response times in the simulation log
- Added verbose logging via logback for all preparing and execution errors

Fixes:
- Fixed potential issue with incorrect consistency level being used
- Reduced Gradle dependencies

Breaking Changes:
- Packages have been moved from `io.github.gatling.dse.*` to `com.datastax.gatling.plugin.*`.
  - Existing usage will require replacing class import packages.  No classes have been renamed.


## [v0.0.8] - Nickel Panda - 2017-04-01
- Updated DSE Driver to latest release (v1.1.2)
- Overhauled utilities used to bind Gatling session vars to CQL bound statements
- Added support for inserting nulls
- Resolved conflict with Guava v16 for downstream project build compatibility
- Group multiple failures under the same error message (by [@pingtimeout](https://github.com/pingtimeout))


## [v0.0.7] - Titanium Pigeon - 2016-12-10
- Added support for Counters, UDTs, Tuples, Date, Small Int, Tiny Int and Time CQL types
- Added log events to enable logging plugin errors to log file in addition to console
- Moved cassandraUnit files to build dir
- Added spec for validating UDTs


## [v0.0.6] - Silver Eagle - 2016-12-07
- Allow parameter names override for Gremlin queries (by [@pingtimeout](https://github.com/pingtimeout))
- Update dependencies to latest versions


## [v0.0.5] - Iron Dolphin - 2016-11-08
- Support for new DataStax Fluent Gremlin Graph API
- Replaced ThreadPooling w/ Akka (by [@snazy](https://github.com/snazy))
- Fixed issue with incorrect response timing metrics (by [@snazy](https://github.com/snazy))
- Removed unneeded classes


## [v0.0.4] - Platinum Owl - 2016-11-04
- New support for passing a List of Gatling session params to withParams()
- Clean up of code to CQL binding of params to prepared query
- Addition of Java docs for many functions
- Support for more CQL/Cassandra types for Named and Prepared statements
- Update of Specs/TestSims for more complete prepared statement testing
- Renaming of executeSimple() to executeCql()


## [v0.0.3] - Aluminum Monkey - 2016-11-03
- Support for Graph Statements w/ named params via .withSetParams()
- Manual spec for running graph simulation tests


## [v0.0.2] - 2016-11-01
- Support for Named Params in prepared statements (by [@jkds](https://github.com/jkds))
- Better error handling with CQL request params binding (by [@jkds](https://github.com/jkds))
- Inclusion of Unit tests w/ CassandraUnit to validate Simulations work correctly
- Updated Gradle configurations to be latest versions, include compile settings and licensing


## [v0.0.1] - 2016-08-14
- Initial pre-release of Gatling DSE Plugin


[Unreleased]: https://github.com/riptano/gatling-dse-plugin/compare/v1.1.1...HEAD
[v1.1.1]: https://github.com/riptano/gatling-dse-plugin/compare/v1.1.0...v1.1.1
[v1.1.0]: https://github.com/riptano/gatling-dse-plugin/compare/v1.0.2...v1.1.0
[v1.0.2]: https://github.com/riptano/gatling-dse-plugin/compare/v1.0.1...v1.0.2
[v1.0.1]: https://github.com/riptano/gatling-dse-plugin/compare/v1.0.0...v1.0.1
[v1.0.0]: https://github.com/riptano/gatling-dse-plugin/compare/v0.0.8...v1.0.0
[v0.0.8]: https://github.com/riptano/gatling-dse-plugin/compare/v0.0.7...v0.0.8
[v0.0.7]: https://github.com/riptano/gatling-dse-plugin/compare/v0.0.6...v0.0.7
[v0.0.6]: https://github.com/riptano/gatling-dse-plugin/compare/v0.0.5...v0.0.6
[v0.0.5]: https://github.com/riptano/gatling-dse-plugin/compare/v0.0.4...v0.0.5
[v0.0.4]: https://github.com/riptano/gatling-dse-plugin/compare/v0.0.3...v0.0.4
[v0.0.3]: https://github.com/riptano/gatling-dse-plugin/compare/v0.0.2...v0.0.3
[v0.0.2]: https://github.com/riptano/gatling-dse-plugin/compare/v0.0.1...v0.0.2
[v0.0.1]: https://github.com/riptano/gatling-dse-plugin/compare/v0.0.1...v0.0.1
