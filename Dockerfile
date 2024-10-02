# Use an official OpenJDK runtime as a parent image
FROM openjdk:17-jdk-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY target/ingestion-0.0.1-SNAPSHOT.jar /app/

# Run the jar file
ENTRYPOINT ["java","-jar","ingestion-0.0.1-SNAPSHOT.jar"]

EXPOSE 8080