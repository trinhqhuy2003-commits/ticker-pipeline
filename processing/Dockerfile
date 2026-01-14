FROM maven:3.9-eclipse-temurin-11 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

FROM eclipse-temurin:11-jre
WORKDIR /app
COPY --from=build /app/target/ticker-processing-1.0-SNAPSHOT.jar app.jar

# We need a start script to handle waiting or just run the jar
CMD ["java", "-jar", "app.jar"]
