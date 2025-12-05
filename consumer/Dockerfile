FROM apache/spark:3.5.1-python3

USER root

# Установка зависимостей
RUN apt-get update && apt-get install -y \
    python3-pip \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Установка Python пакетов
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir -r /tmp/requirements.txt

# Настройка Spark
ENV SPARK_HOME=/opt/spark
ENV PYSPARK_PYTHON=python3
ENV PATH=$PATH:$SPARK_HOME/bin

# Создание директории для чекпоинтов
RUN mkdir -p /tmp/checkpoint && chmod 777 /tmp/checkpoint

# Создание директории для Ivy кэша
RUN mkdir -p /home/spark/.ivy2/cache && chown -R 185:185 /home/spark/.ivy2 && chmod -R 777 /home/spark/.ivy2

# Используем существующего пользователя spark (UID 185)
USER 185
