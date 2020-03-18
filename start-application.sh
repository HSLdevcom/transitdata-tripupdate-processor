#!/bin/bash

if [[ -z "${DEBUG_ENABLED}" ]]; then
  java -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 -Xms256m -Xmx4096m -jar /usr/app/transitdata-tripupdate-processor.jar
else
  java -Xms256m -Xmx4096m -jar /usr/app/transitdata-tripupdate-processor.jar
fi
