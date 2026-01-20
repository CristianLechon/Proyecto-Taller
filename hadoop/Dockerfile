FROM ubuntu:22.04

# Argumentos y Variables de Entorno
ARG HADOOP_VERSION=3.4.2
ENV HADOOP_HOME=/usr/local/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Instalación de Apache Spark
ARG SPARK_VERSION=3.5.7
ENV SPARK_HOME=/usr/local/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Instalación de dependencias
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk ssh rsync wget vim sudo net-tools iputils-ping && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Descargar e instalar Hadoop
RUN wget https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz -O /tmp/hadoop.tar.gz && \
    tar -xzf /tmp/hadoop.tar.gz -C /usr/local && \
    mv /usr/local/hadoop-${HADOOP_VERSION} $HADOOP_HOME && \
    rm /tmp/hadoop.tar.gz

# Descargar e instalar Spark
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -O /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /usr/local && \
    mv /usr/local/spark-${SPARK_VERSION}-bin-hadoop3 $SPARK_HOME && \
    rm /tmp/spark.tgz

# Configuración de SSH (Sin contraseña para root)
RUN ssh-keygen -t rsa -P '' -f /root/.ssh/id_rsa && \
    cat /root/.ssh/id_rsa.pub >> /root/.ssh/authorized_keys && \
    chmod 0600 /root/.ssh/authorized_keys

# Definir usuarios para evitar errores en scripts de inicio
ENV HDFS_NAMENODE_USER=root
ENV HDFS_DATANODE_USER=root
ENV HDFS_SECONDARYNAMENODE_USER=root
ENV YARN_RESOURCEMANAGER_USER=root
ENV YARN_NODEMANAGER_USER=root

# Configurar variables para que PySpark use Python3
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Prompt más claro
RUN echo "export PS1='[\u@\h] \$ '" >> /root/.bashrc
# Script de inicio
# Este script actualiza /etc/hosts dinámicamente al arrancar el contenedor
RUN echo '#!/bin/bash\n\
service ssh start\n\
# Esperar unos segundos para la red\n\
sleep 2\n\
# Loop infinito para mantener el contenedor vivo\n\
tail -f /dev/null' > /usr/local/bin/start-hadoop.sh && \
chmod +x /usr/local/bin/start-hadoop.sh

EXPOSE 9870 9864 8088 8042 4040 22

CMD ["/usr/local/bin/start-hadoop.sh"]