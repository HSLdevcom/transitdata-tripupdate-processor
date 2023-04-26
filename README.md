# Transitdata-tripupdate-processor [![Test and create Docker image](https://github.com/HSLdevcom/transitdata-tripupdate-processor/actions/workflows/test-and-build.yml/badge.svg)](https://github.com/HSLdevcom/transitdata-tripupdate-processor/actions/workflows/test-and-build.yml)

This project is part of the [Transitdata Pulsar-pipeline](https://github.com/HSLdevcom/transitdata).

## Description

This application creates GTFS-RT Trip Update messages by combining individual stop estimates. Stop cancellation messages are also applied to create cancellations in GTFS-RT format. This application reads multiple Pulsar topics and publishes messages to one topic.

## Building

### Dependencies

This project depends on [transitdata-common](https://github.com/HSLdevcom/transitdata-common) project.

### Locally

- `mvn compile`
- `mvn package`

### Docker image

- Run [this script](build-image.sh) to build the Docker image

### Tests

We're separating our unit & integration tests using [this pattern](https://www.petrikainulainen.net/programming/maven/integration-testing-with-maven/).

Unit tests:

- add test classes under ./src/test with suffix *Test.java
- `mvn clean test -P unit-test`   

Integration tests:

- add test classes under ./src/integration-test with prefix IT*.java
- `mvn clean verify -P integration-test`   

## Running

### Dependencies

* Pulsar

### Environment variables

* `TRIPUPDATE_MAX_AGE_IN_SECS`: maximum age of the trip update. If data is older than this, no trip update is created
* `TRIPUPDATE_MIN_TIME_BEFORE_DEPARTURE_IN_SECS`: maximum amount of time before the scheduled departure time of the trip
* `TRIPUPDATE_MAX_MISSING_ESTIMATES`: maximum number of missing estimates. If higher than this, no trip update is created
  * The main use case of this environment variable is to filter bad metro estimates
* `TRIPUPDATE_TIMEZONE`: timezone to use in the trip update
* `FILTER_TRAIN_DATA`: whether to filter data for trains. If true, no trip updates are created for trains
* `PUBLISHER_DEBOUNCE_DELAY`: debounce delay period when publishing trip updates
  * This environment variable is used to limit the amount of trip updates published because stop estimates tend to arrive in bursts

