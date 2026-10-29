FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -B -q dependency:go-offline
COPY src ./src
RUN mvn -B -q -DskipTests package dependency:copy-dependencies -DincludeScope=runtime

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /build/target/netty-codec-redis-*.jar app.jar
COPY --from=build /build/target/dependency/ lib/
ENV JAVA_OPTS=""
EXPOSE 6379
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -Dport=6379 -cp 'app.jar:lib/*' io.netty.example.server.RedisServer"]
