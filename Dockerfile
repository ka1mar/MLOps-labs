FROM bitnami/spark:3.5.1

USER root

# Install specific Scala version
ENV SCALA_VERSION=2.12.18
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    unzip && \
    curl -fsL https://downloads.typesafe.com/scala/$SCALA_VERSION/scala-$SCALA_VERSION.deb -o scala.deb && \
    apt-get install -y ./scala.deb && \
    rm scala.deb

# Download MySQL connector with correct coordinates
RUN mkdir -p /app/jars && \
    curl -o /app/jars/mysql-connector-java-8.0.33.jar \
      https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar && \
    unzip -t /app/jars/mysql-connector-java-8.0.33.jar

# Copy and compile DataMart
COPY datamart/src/DataMart.scala /app/datamart/
WORKDIR /app/datamart

# Compile with Java 8 compatibility
RUN mkdir -p classes && \
    scalac -J-Xmx2g -cp "/opt/bitnami/spark/jars/*:/app/jars/*" -d classes DataMart.scala && \
    jar cf datamart.jar -C classes . && \
    mv datamart.jar /app/jars/

# Copy Python code
COPY scripts /app/scripts

RUN pip install numpy mysql-connector-python

RUN chown -R 1001:root /app
USER 1001

WORKDIR /app

