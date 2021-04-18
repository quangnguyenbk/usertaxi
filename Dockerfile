FROM docker.io/library/openjdk:8-jdk-alpine

WORKDIR /

COPY ./target/test-0.0.1-SNAPSHOT.jar ./test-0.0.1-SNAPSHOT.jar

EXPOSE 8500

RUN apk update && apk add --no-cache gcompat

ENTRYPOINT ["java","-jar","/test-0.0.1-SNAPSHOT.jar"]