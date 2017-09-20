ARG MAVEN_VERSION=3-jdk-8
ARG JDK_VERSION=8-jre

FROM maven:$MAVEN_VERSION AS builder

ARG NOTEBOOK_VERSION=0.7.0-pre2
ARG SCALA_VERSION=2.10.5
ARG SPARK_VERSION=1.6.3
ARG HADOOP_VERSION=2.6

# Spark for commandline
RUN wget http://www-us.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz \
    && mkdir /spark \
    && tar xfz spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz -C /spark \
    && rm spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz

# Spark Notebook
RUN wget https://s3.eu-central-1.amazonaws.com/spark-notebook/tgz/spark-notebook-$NOTEBOOK_VERSION-scala-$SCALA_VERSION-spark-$SPARK_VERSION-hadoop-$HADOOP_VERSION.0.tgz \
    && mkdir /notebook \
    && tar xfz spark-notebook-$NOTEBOOK_VERSION-scala-$SCALA_VERSION-spark-$SPARK_VERSION-hadoop-$HADOOP_VERSION.0.tgz -C /notebook \
    && rm spark-notebook-$NOTEBOOK_VERSION-scala-$SCALA_VERSION-spark-$SPARK_VERSION-hadoop-$HADOOP_VERSION.0.tgz

# Scala
RUN wget https://downloads.lightbend.com/scala/$SCALA_VERSION/scala-$SCALA_VERSION.tgz \
    && mkdir /scala \
    && tar xfz scala-$SCALA_VERSION.tgz -C /scala \
    && rm scala-$SCALA_VERSION.tgz

# Warcbase
COPY . /aut
RUN cd /aut && mvn clean install

# Clean slate for smaller final image build
FROM openjdk:$JDK_VERSION
LABEL maintainer="Web Science and Digital Libraries Research Group <https://twitter.com/WebSciDL>"

ENV SCALA_HOME=/usr/share/scala \
    SPARK_HOME=/spark \
    ADD_JARS=/aut/aut-fatjar.jar

EXPOSE 9001
VOLUME /notes /data
WORKDIR /notes

COPY --from=builder /notebook/* /notebook/
COPY --from=builder /spark/* /spark/
COPY --from=builder /scala/* /usr/share/scala/
COPY --from=builder /aut/target/aut-*-fatjar.jar /aut/aut-fatjar.jar

RUN ln -s /usr/share/scala/bin/scala /usr/bin/scala \
    && ln -s /spark/bin/spark-shell /usr/bin/spark-shell \
    && ln -s /notebook/bin/spark-notebook /usr/bin/spark-notebook

CMD spark-notebook
