FROM gradle:8.7-jdk17 AS builder

WORKDIR /build

COPY build.gradle .
COPY src ./src

RUN gradle build --no-daemon -x test

FROM eclipse-temurin:21-jre-alpine

RUN addgroup -g 1000 -S vertx && \
    adduser -u 1000 -S vertx -G vertx

WORKDIR /app

COPY --from=builder --chown=vertx:vertx /build/build/libs/*.jar app.jar

RUN mkdir -p /mnt/nfs && \
    mkdir -p /tmp/file-uploads && \
    mkdir -p /tmp/infinispan-indexes && \
    chown -R vertx:vertx /mnt/nfs /tmp/file-uploads /tmp/infinispan-indexes && \
    chmod 755 /tmp/file-uploads /tmp/infinispan-indexes

USER vertx:vertx

EXPOSE 8080 8079 7800

ENTRYPOINT ["java", "-jar", "-Djava.io.tmpdir=/tmp", "app.jar"]