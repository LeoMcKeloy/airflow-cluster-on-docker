FROM apache/airflow:2.5.0

USER root

RUN apt update && \
    apt-get install -y openjdk-11-jdk ant openssh-client && \
    mkdir /root/.ssh && \
    apt-get clean;

COPY ./id_rsa /root/.ssh/

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

USER airflow