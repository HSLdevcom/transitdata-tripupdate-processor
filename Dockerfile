FROM eclipse-temurin:11-alpine
#Install curl for health check
RUN apk add --no-cache curl

ADD target/transitdata-tripupdate-processor-jar-with-dependencies.jar /usr/app/transitdata-tripupdate-processor.jar
COPY start-application.sh /
RUN chmod +x /start-application.sh

CMD ["/start-application.sh"]
