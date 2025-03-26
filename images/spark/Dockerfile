#+-----------------------------------------+
#|           Imagem Spark 3.5.3            | 
#|                                         |
#| O que decide se é Driver ou Worker      |
#| é o comando de inicialização no compose |
#+-----------------------------------------+

FROM ubuntu:22.04

SHELL ["/bin/bash", "-c"]

RUN apt-get update --fix-missing

#+-----------------+
#| Instalando cURL |
#+-----------------+

RUN apt-get install curl -y

#+--------------------+
#| Instalando Java 11 |
#+--------------------+

RUN apt-get install openjdk-18-jdk -y                                           && \
    echo "export JAVA_HOME=/usr/lib/jvm/java-18-openjdk-amd64" >> ~/.bashrc     && \
    echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> ~/.bashrc

ENV JAVA_HOME /usr/lib/jvm/java-18-openjdk-amd64
ENV PATH "${JAVA_HOME}/bin:$PATH"

#+------------------------+
#| Instalando Scala 2.12  |
#+------------------------+

RUN curl -O https://downloads.lightbend.com/scala/2.12.18/scala-2.12.18.tgz &&\
    tar zxvf scala-2.12.18.tgz -C /usr/local &&\
    rm scala-2.12.18.tgz &&\
    cd /usr/local &&\
    ln -sT scala-2.12.18 scala

RUN echo "export SCALA_HOME=/usr/local/scala" >> ~/.bashrc
RUN echo "export PATH=\$SCALA_HOME/bin:\$PATH" >> ~/.bashrc

ENV SCALA_HOME /usr/local/scala
ENV PATH "${SCALA_HOME}/bin:$PATH"

 #+------------------------+
 #| Instalando Spark 3.5.3 |
 #+------------------------+

RUN curl -O https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz &&\
    tar zxvf spark-3.5.0-bin-hadoop3.tgz -C /usr/local &&\
    rm spark-3.5.0-bin-hadoop3.tgz &&\
    cd /usr/local &&\
    ln -sT spark-3.5.0-bin-hadoop3 spark

RUN echo "export SPARK_HOME=/usr/local/spark" >> ~/.bashrc
RUN echo "export PATH=\$SPARK_HOME/bin:\$PATH" >> ~/.bashrc

ENV SPARK_HOME /usr/local/spark
ENV PATH "${SPARK_HOME}/bin:$PATH" 

#+----------------------------------+
#| Instalando dependências S3/MinIO |
#+----------------------------------+

RUN curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    --output ${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar

RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    --output ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.12.262.jar

RUN curl https://jdbc.postgresql.org/download/postgresql-42.5.0.jar \
    --output ${SPARK_HOME}/jars/postgresql-42.5.0.jar

#+---------------------------------------------+
#| Instalando dependências para Delta Lake     |
#+---------------------------------------------+

RUN curl https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.0.0/delta-core_2.12-3.0.0.jar \
    --output ${SPARK_HOME}/jars/delta-core_2.12-3.0.0.jar

RUN curl https://repo1.maven.org/maven2/io/delta/delta-storage/2.0.0/delta-storage-3.0.0.jar \
    --output ${SPARK_HOME}/jars/delta-storage-3.0.0.jar

USER root