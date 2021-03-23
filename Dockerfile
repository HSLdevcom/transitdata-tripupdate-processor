FROM openjdk:8-jre-slim

# curl for health check
RUN apt-get update && apt-get install -y --no-install-recommends curl

ADD target/transitdata-tripupdate-processor.jar /usr/app/transitdata-tripupdate-processor.jar
COPY start-application.sh /
RUN chmod +x /start-application.sh

CMD ["/start-application.sh"]
