:warning: **This plugin is no longer maintained.  Users should consider migrating to [nosqlbench](https://github.com/nosqlbench/nosqlbench).**

# Gatling DSE Plugin

This project is a plugin for the Gatling load injector.
It adds CQL support in Gatling for Datastax Enterprise.
It allows for benchmarking Datastax Enterprise features, including DSE Graph Fluent API.

## Building

To build this plugin, you will need Java 8 or later and SBT.

During development, start an SBT shell by just running `sbt` and keep it open.
Then, run `compile` to compile the sources and `test` to execute all unit tests.

Before checking in any new file, make sure the licence headers have been added by running `headerCheck` in the SBT shell.
To add them automatically in all newly created file, run `headerCreate` in the SBT shell.

To build the plugin as an uberjar, run `sbt assembly`.
The plugin jar will be in the `target/scala-2.12/` directory.

## Installation

### With a Gatling bundle

Build the plugin as an uberjar.
Copy it into Gatling `lib` folder.

### With other build systems

You may also rely on Gatling Maven or Gradle plugins to launch your tests.
In this case, include the plugin as a dependency in your project.

## More Information on Usage

Gatling documentation is available at the following locations:

- [Gatling Quickstart](http://gatling.io/docs/current/quickstart/)
- [Gatling Cheatsheet](http://gatling.io/docs/current/cheat-sheet/)

## Contributions

This project was inspired by Mikhail Stepura ([Mishail](https://github.com/Mishail)'s project [GatlingCql](https://github.com/gatling-cql/GatlingCql/commits/master)).

It has been developped by Brad Vernon ([ibspoof](https://github.com/ibspoof)) and improved by the following contributors:

* Matt Stump ([mstump](https://github.com/mstump))
* James Kavanagh ([jkds](https://github.com/jkds))
* Robert Stupp ([snazy](https://github.com/snazy))
* Pierre Laporte ([pingtimeout](https://github.com/pingtimeout))
