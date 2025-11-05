# Use OpenJDK for running the application
FROM openjdk:26-trixie
WORKDIR /app
COPY target/Orchestration-Service-0.0.1-SNAPSHOT.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]                 