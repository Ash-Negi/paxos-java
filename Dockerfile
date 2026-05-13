# Build stage: compile and assemble the fat jar.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml ./
COPY paxos-core/pom.xml paxos-core/pom.xml
COPY server/pom.xml server/pom.xml
COPY kvstore/pom.xml kvstore/pom.xml
COPY paxos-core/src paxos-core/src
COPY server/src server/src
COPY kvstore/src kvstore/src
RUN mvn -q -DskipTests -pl server -am package

# Runtime stage: JRE only.
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /src/server/target/server-1.0.0-SNAPSHOT-jar-with-dependencies.jar /app/server.jar
EXPOSE 8080 9100
ENTRYPOINT ["java", "-jar", "/app/server.jar"]
