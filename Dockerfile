FROM openjdk:8-slim
WORKDIR /vance
COPY S3-1.0-SNAPSHOT-jar-with-dependencies.jar /vance
CMD ["mkdir", "/root/.aws"]
CMD ["java", "-jar", "./S3-1.0-SNAPSHOT-jar-with-dependencies.jar"]