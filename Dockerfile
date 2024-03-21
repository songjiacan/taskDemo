FROM maven:3.8.4-openjdk-11 AS build

# Set the working directory
WORKDIR /app

# Copy the Maven project files into the container
COPY pom.xml .
COPY src ./src

# Build the Maven project
RUN mvn package

# Use Java 8 base image for running the application
FROM openjdk:8-jre-slim

# Set the working directory 
WORKDIR /app

# Copy the JAR file to the container
COPY --from=build /app/target/taskDemo-*.jar app.jar

# Command to run the application
CMD ["java", "-jar", "app.jar"]