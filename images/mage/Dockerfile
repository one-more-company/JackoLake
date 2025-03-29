FROM ubuntu:22.04

SHELL ["/bin/bash", "-c"]
RUN apt update
RUN apt-get update --fix-missing
RUN apt-get install openjdk-18-jdk -y  

#+-----------------+
#| Installing cURL |
#+-----------------+

RUN apt-get install curl -y

# #+------------------------+
# #| Installing Python pip |
# #+------------------------+

RUN apt-get install python3-pip -y
RUN pip install --upgrade pip


#+-----------------+
#| Set up mageai   |
#+-----------------+
RUN pip install mage-ai

ARG USER_CODE_PATH=/home/src/${PROJECT_NAME}

# RUN mage start /home/src/${PROJECT_NAME}

ARG USER_CODE_PATH=/home/src/${PROJECT_NAME}
# Note: this overwrites the requirements.txt file in your new project on first run.
# COPY ./mage/requirements.txt ${USER_CODE_PATH}requirements.txt 
# RUN pip3 install -r ${USER_CODE_PATH}requirements.txt

#+--------------------+
#| Installing PySpark |
#+--------------------+

RUN pip install pyspark==3.5.0

ENV SPARK_HOME=/usr/local/lib/python3.10/dist-packages/pyspark

RUN mkdir -p usr/local/spark_dev/work/

#+----------------------------------+
#| Installing S3/MinIO dependencies |
#+----------------------------------+

RUN curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    --output $SPARK_HOME/jars/hadoop-aws-3.3.4.jar
RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    --output $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.262.jar
RUN curl https://jdbc.postgresql.org/download/postgresql-42.5.0.jar \
    --output $SPARK_HOME/jars/postgresql-42.5.0.jar

#+--------------------------------------------------+
#| Downloading Hive Metastore 3.0.0 dependencies    |
#|                                                  |
#|   Obtained using these configurations:           |
#|                                                  |
#|    spark.sql.hive.metastore.jars=maven           |
#|    spark.sql.hive.metastore.version=3.0.0        |
#|                                                  |
#|   start a SparkSession with these configs        |
#|   and execute any spark.sql() command            |
#|                                                  |
#+--------------------------------------------------+

RUN mkdir -p ${SPARK_HOME}/hms-3.0.0/jars
COPY conf/hms-3.0.0-deps.txt ${SPARK_HOME}/hms-3.0.0/jars
RUN cd $SPARK_HOME/hms-3.0.0/jars && xargs -n1 -P1 curl --remote-name < hms-3.0.0-deps.txt


#+------------------------------------+
#| Instalando NodeJS (para extensões) |
#+------------------------------------+

RUN apt-get install nodejs -y
RUN apt-get install npm -y


#+----------------+
#| Installing Git |
#+----------------+

RUN apt-get install git -y
RUN pip install jupyterlab-git


# Apache Hive

#+-----------------------------------------------+
#| Configurations for  spark-defaults            |
#+-----------------------------------------------+

COPY conf/spark-defaults.conf ${SPARK_HOME}/conf/

RUN chmod -R 777 /usr/local/spark_dev/work
USER root
