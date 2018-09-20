[![Build Status](https://travis-ci.org/HSLdevcom/transitdata-tripupdate-processor.svg?branch=master)](https://travis-ci.org/HSLdevcom/transitdata-tripupdate-processor)

## Description

Application for creating GTFS-RT *Trip Update*-messages and publishing them to Pulsar topic(s).

More system-level documentation can be found in [this project](https://gitlab.hsl.fi/transitdata/transitdata-doc).

## Building

### Dependencies

This project depends on [transitdata-common](https://github.com/HSLdevcom/transitdata-common) project.

### Locally

- Build and install common lib to local maven repository before compiling this one.
  - ```cd transitdata-common && mvn install```  
- ```mvn compile```  
- ```mvn package```  

### Docker image

- Run [this script](build-image.sh) to build the Docker image


## Running

Requirements:
- Local Pulsar Cluster
  - By default uses localhost, override host in PULSAR_HOST if needed.
    - Tip: f.ex if running inside Docker in OSX set `PULSAR_HOST=host.docker.internal` to connect to the parent machine
  - You can use [this script](https://gitlab.hsl.fi/transitdata/transitdata-doc/bin/pulsar/pulsar-up.sh) to launch it as Docker container

Launch Docker container with

```docker-compose -f compose-config-file.yml up <service-name>```   

See [the documentation-project](https://gitlab.hsl.fi/transitdata/transitdata-doc) for details
