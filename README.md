[![Build Status](https://travis-ci.org/HSLdevcom/transitdata-tripupdate-processor.svg?branch=master)](https://travis-ci.org/HSLdevcom/transitdata-tripupdate-processor)

# Transitdata-tripupdate-processor

This project is part of the [Transitdata Pulsar-pipeline](https://github.com/HSLdevcom/transitdata).

## Description

Application for creating GTFS-RT *Trip Update*-messages and publishing them to Pulsar topic(s).

## Building

### Dependencies

This project depends on [transitdata-common](https://github.com/HSLdevcom/transitdata-common) project.

### Locally

- ```mvn compile```  
- ```mvn package```  

### Docker image

- Run [this script](build-image.sh) to build the Docker image


## Running

Requirements:
- Local Pulsar Cluster
  - By default uses localhost, override host in PULSAR_HOST if needed.
    - Tip: f.ex if running inside Docker in OSX set `PULSAR_HOST=host.docker.internal` to connect to the parent machine
  - You can use [this script](https://github.com/HSLdevcom/transitdata/blob/master/bin/pulsar/pulsar-up.sh) to launch it as Docker container

Launch Docker container with

```docker-compose -f compose-config-file.yml up <service-name>```   



## Tests:

We're separating our unit & integration tests using [this pattern](https://www.petrikainulainen.net/programming/maven/integration-testing-with-maven/).

Unit tests:

- add test classes under ./src/test with suffix *Test.java
- `mvn clean test -P unit-test`   

Integration tests:

- add test classes under ./src/integration-test with prefix IT*.java
- `mvn clean verify -P integration-test`   

