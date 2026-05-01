FROM bitnami/spark:3.5.1

USER root
RUN useradd -m -u 1001 sparkuser

# Install dependencies
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    unzip \
    default-jdk \
    wget && \
    rm -rf /var/lib/apt/lists/*

# Install Scala manually
ENV SCALA_VERSION=2.12.18
ENV SCALA_HOME=/usr/share/scala
RUN wget -q https://downloads.lightbend.com/scala/${SCALA_VERSION}/scala-${SCALA_VERSION}.tgz && \
    tar -xzf scala-${SCALA_VERSION}.tgz && \
    mkdir -p ${SCALA_HOME} && \
    mv scala-${SCALA_VERSION}/* ${SCALA_HOME}/ && \
    rm -rf scala-${SCALA_VERSION} scala-${SCALA_VERSION}.tgz && \
    ln -s ${SCALA_HOME}/bin/scala /usr/local/bin/scala && \
    ln -s ${SCALA_HOME}/bin/scalac /usr/local/bin/scalac

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

