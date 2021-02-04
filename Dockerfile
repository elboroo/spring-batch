FROM openjdk:11-jre-slim-buster
ADD target/batch.jar /
CMD java -jar \
    -Dserver.port=$SERVER_PORT \
    -Dspring.datasource.url=$DATABASE_URL \
    batch.jar
