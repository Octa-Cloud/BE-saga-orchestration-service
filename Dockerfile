# ---- Build stage ----
FROM gradle:8.7-jdk17 AS build
WORKDIR /workspace
COPY build.gradle settings.gradle ./
COPY gradle/ gradle/
RUN gradle --no-daemon dependencies
COPY src/ src/
RUN gradle --no-daemon clean bootJar -x test

# ---- Runtime stage ----
FROM eclipse-temurin:17-jre-jammy
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*
RUN useradd -ms /bin/bash appuser
WORKDIR /app
ARG JAR_FILE=build/libs/*.jar
COPY --from=build /workspace/${JAR_FILE} app.jar
USER appuser
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD curl -fsS http://localhost:8080/actuator/health || exit 1
ENTRYPOINT ["java","-XX:MaxRAMPercentage=75.0","-XX:+UseG1GC","-XX:+UseStringDeduplication","-Djava.security.egd=file:/dev/./urandom","-jar","app.jar"]