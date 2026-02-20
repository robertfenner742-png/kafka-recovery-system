FROM maven:3.9.6-eclipse-temurin-21 AS build

WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean -B -q package -DskipTests

FROM eclipse-temurin:21-jre-alpine

ENV JAVA_OPTS="-Xms512m -Xmx1024m\
    -XX:+UseContainerSupport\
    -XX:MaxRAMPercentage=75.0\
    -XX:+UseG1GC\
    -XX:+HeapDumpOnOutOfMemoryError\
    -XX:+ExitOnOutOfMemoryError\
    -XX:HeapDumpPath=/app/heapdumps\
    -Djava.security.egd=file:/dev/./urandom"

WORKDIR /app
COPY --from=build /app/target/distributer-kafka-consumer-0.0.1-SNAPSHOT.jar app.jar

RUN addgroup -g 1000 appuser && \
    adduser -u 1000 -G appuser -h /app -s /bin/sh -D appuser && \
    mkdir -p /app/heapdumps && \
    chown -R appuser:appuser /app

USER appuser

EXPOSE 8080

ENTRYPOINT ["sh", "-c", "export POD_NAME=$HOSTNAME && java $JAVA_OPTS -jar app.jar"]