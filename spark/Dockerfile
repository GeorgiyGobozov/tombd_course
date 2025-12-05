FROM apache/spark:3.5.1-python3

USER root

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Установка Python пакетов (минимальный набор)
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir -r /tmp/requirements.txt

# Настройка Spark
ENV SPARK_HOME=/opt/spark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PATH=$PATH:$SPARK_HOME/bin
ENV SPARK_USER=spark
ENV HADOOP_USER_NAME=spark

# Создание директорий
RUN mkdir -p /tmp/spark-events /opt/spark/logs $SPARK_HOME/conf
RUN chmod 777 /tmp/spark-events /opt/spark/logs $SPARK_HOME/conf

# Копирование конфигурации Spark
COPY spark-defaults.conf $SPARK_HOME/conf/

# Создание директории для Ivy кэша
RUN mkdir -p /home/spark/.ivy2/cache && chown -R 185:185 /home/spark/.ivy2

# Отключение Hadoop security (ключевое исправление!)
RUN echo "" >> $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.hadoop.hadoop.security.authentication simple" >> $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.authenticate false" >> $SPARK_HOME/conf/spark-defaults.conf && \
    echo "spark.acls.enable false" >> $SPARK_HOME/conf/spark-defaults.conf

# Используем существующего пользователя spark (не создаём нового)
USER 185

EXPOSE 7077 8080 4040 18080 8081 8082