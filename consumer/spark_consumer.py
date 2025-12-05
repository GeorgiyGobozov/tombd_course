import os
import sys
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Добавьте путь Spark в sys.path
spark_home = os.environ.get('SPARK_HOME', '/opt/spark')
python_lib = os.path.join(spark_home, 'python')
py4j_path = None

# Ищем файл py4j
for f in os.listdir(os.path.join(spark_home, 'python/lib')):
    if f.startswith('py4j') and f.endswith('.zip'):
        py4j_path = os.path.join(spark_home, 'python/lib', f)
        break

# Добавляем пути
if python_lib not in sys.path:
    sys.path.insert(0, python_lib)
if py4j_path and py4j_path not in sys.path:
    sys.path.insert(0, py4j_path)

print(f"Spark home: {spark_home}")
print(f"Python lib: {python_lib}")
print(f"Py4j path: {py4j_path}")
print(f"Sys.path: {sys.path}")

try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import *
    from pyspark.sql.types import *
    from pyspark.sql.window import Window
    print("PySpark импортирован успешно")
except ImportError as e:
    print(f"Ошибка импорта PySpark: {e}")
    print("Убедитесь, что pyspark установлен в образе")
    sys.exit(1)

class BigDataStreamingConsumer:
    def __init__(self):
        # Получение настроек из переменных окружения
        self.spark_master = os.getenv('SPARK_MASTER', 'local[*]')
        self.kafka_broker = os.getenv('KAFKA_BROKER', 'kafka:9092')
        self.kafka_topic = os.getenv('KAFKA_TOPIC', 'user_transactions')
        self.postgres_url = os.getenv('POSTGRES_URL', 'jdbc:postgresql://postgres:5432/bigdata_db')
        self.postgres_user = os.getenv('POSTGRES_USER', 'admin')
        self.postgres_password = os.getenv('POSTGRES_PASSWORD', 'admin123')

        print(f"Инициализация BigDataStreamingConsumer:")
        print(f"  Master: {self.spark_master}")
        print(f"  Kafka: {self.kafka_broker}")
        print(f"  Topic: {self.kafka_topic}")

        # Создание Spark сессии
        self.spark = self.create_spark_session()
        print("Spark session created in __init__")

        # Определение схем данных
        self.schemas = self._define_schemas()
        print("Schemas defined in __init__")
        
    def create_spark_session(self):
        """Создание Spark сессии с настройками"""
        print(f"Создание Spark сессии с мастером: {self.spark_master}")
        
        try:
            spark = SparkSession.builder \
                .appName("BigDataStreamingConsumer") \
                .master(self.spark_master) \
                .config("spark.jars.packages",
                       "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                       "org.postgresql:postgresql:42.7.0") \
                .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
                .config("spark.driver.memory", "2g") \
                .config("spark.executor.memory", "2g") \
                .config("spark.driver.extraJavaOptions", "-Dhadoop.user.name=spark") \
                .config("spark.executor.extraJavaOptions", "-Dhadoop.user.name=spark") \
                .config("spark.kafka.sasl.enabled", "false") \
                .getOrCreate()
            
            spark.sparkContext.setLogLevel("WARN")
            print("Spark сессия создана успешно")
            return spark
            
        except Exception as e:
            print(f"Ошибка при создании Spark сессии: {e}")
            raise
    
    def _define_schemas(self) -> Dict[str, StructType]:
        """Определение всех схем данных"""
        return {
            # Схема для транзакций
            "transaction": StructType([
                StructField("transaction_id", StringType(), True),
                StructField("user_id", StringType(), True),
                StructField("user_name", StringType(), True),
                StructField("user_city", StringType(), True),
                StructField("user_age", IntegerType(), True),
                StructField("user_email", StringType(), True),
                StructField("transaction_amount", DoubleType(), True),
                StructField("transaction_currency", StringType(), True),
                StructField("transaction_category", StringType(), True),
                StructField("merchant", StringType(), True),
                StructField("transaction_date", TimestampType(), True),
                StructField("payment_method", StringType(), True),
                StructField("status", StringType(), True),
                StructField("device", StringType(), True),
                StructField("ip_address", StringType(), True),
                StructField("metadata", MapType(StringType(), StringType()), True),
                StructField("data_type", StringType(), True),
                StructField("batch_id", StringType(), True)
            ]),
            
            # Схема для активности пользователей
            "activity": StructType([
                StructField("activity_id", StringType(), True),
                StructField("user_id", StringType(), True),
                StructField("user_name", StringType(), True),
                StructField("activity_type", StringType(), True),
                StructField("page_url", StringType(), True),
                StructField("session_duration", IntegerType(), True),
                StructField("timestamp", TimestampType(), True),
                StructField("device_info", MapType(StringType(), StringType()), True),
                StructField("location", MapType(StringType(), StringType()), True),
                StructField("data_type", StringType(), True),
                StructField("batch_id", StringType(), True)
            ]),
            
            # Схема для данных датчиков IoT
            "sensor": StructType([
                StructField("sensor_id", StringType(), True),
                StructField("sensor_type", StringType(), True),
                StructField("sensor_location", StringType(), True),
                StructField("timestamp", TimestampType(), True),
                StructField("value", DoubleType(), True),
                StructField("unit", StringType(), True),
                StructField("status", StringType(), True),
                StructField("battery_level", IntegerType(), True),
                StructField("signal_strength", IntegerType(), True),
                StructField("data_type", StringType(), True),
                StructField("batch_id", StringType(), True)
            ])
        }
    
    def _create_postgres_tables(self):
        """Создание необходимых таблиц в PostgreSQL"""
        import psycopg2
        
        try:
            conn = psycopg2.connect(
                host="postgres",
                database="bigdata_db",
                user=self.postgres_user,
                password=self.postgres_password
            )
            conn.autocommit = True
            cursor = conn.cursor()
            
            # Таблица для статистики транзакций по часам
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS transaction_hourly_stats (
                    id SERIAL PRIMARY KEY,
                    window_start TIMESTAMP NOT NULL,
                    window_end TIMESTAMP NOT NULL,
                    user_city VARCHAR(100),
                    transaction_category VARCHAR(100),
                    transaction_count INTEGER,
                    total_amount DECIMAL(12, 2),
                    avg_amount DECIMAL(10, 2),
                    min_amount DECIMAL(10, 2),
                    max_amount DECIMAL(10, 2),
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(window_start, window_end, user_city, transaction_category)
                )
            """)
            
            # Таблица для топ пользователей
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS top_users (
                    id SERIAL PRIMARY KEY,
                    window_start TIMESTAMP NOT NULL,
                    window_end TIMESTAMP NOT NULL,
                    user_id VARCHAR(100),
                    user_name VARCHAR(200),
                    user_city VARCHAR(100),
                    total_spent DECIMAL(12, 2),
                    transaction_count INTEGER,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(window_start, window_end, user_id)
                )
            """)
            
            # Таблица для статистики активности
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS activity_stats (
                    id SERIAL PRIMARY KEY,
                    window_start TIMESTAMP NOT NULL,
                    window_end TIMESTAMP NOT NULL,
                    activity_type VARCHAR(50),
                    city VARCHAR(100),
                    activity_count INTEGER,
                    avg_session_duration DECIMAL(10, 2),
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(window_start, window_end, activity_type, city)
                )
            """)
            
            # Таблица для аномалий датчиков
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sensor_anomalies (
                    id SERIAL PRIMARY KEY,
                    window_start TIMESTAMP NOT NULL,
                    window_end TIMESTAMP NOT NULL,
                    sensor_id VARCHAR(100),
                    sensor_type VARCHAR(50),
                    sensor_location VARCHAR(100),
                    status VARCHAR(20),
                    reading_count INTEGER,
                    avg_value DECIMAL(10, 3),
                    std_value DECIMAL(10, 3),
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(window_start, window_end, sensor_id)
                )
            """)
            
            # Таблица для метрик по методам оплаты
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS payment_method_stats (
                    id SERIAL PRIMARY KEY,
                    window_start TIMESTAMP NOT NULL,
                    window_end TIMESTAMP NOT NULL,
                    payment_method VARCHAR(50),
                    transaction_count INTEGER,
                    total_amount DECIMAL(12, 2),
                    avg_amount DECIMAL(10, 2),
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(window_start, window_end, payment_method)
                )
            """)
            
            # Таблица для метрик по устройствам
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS device_stats (
                    id SERIAL PRIMARY KEY,
                    window_start TIMESTAMP NOT NULL,
                    window_end TIMESTAMP NOT NULL,
                    device_type VARCHAR(50),
                    transaction_count INTEGER,
                    success_rate DECIMAL(5, 2),
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(window_start, window_end, device_type)
                )
            """)
            
            # Таблица для сводных метрик по часам
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS hourly_metrics (
                    id SERIAL PRIMARY KEY,
                    hour TIMESTAMP NOT NULL UNIQUE,
                    total_transactions INTEGER,
                    total_amount DECIMAL(12, 2),
                    active_users INTEGER,
                    avg_transaction_amount DECIMAL(10, 2),
                    anomaly_count INTEGER,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cursor.close()
            conn.close()
            logger.info("Таблицы в PostgreSQL созданы/проверены")
            
        except Exception as e:
            logger.error(f"Ошибка при создании таблиц в PostgreSQL: {e}")
            raise
    
    def read_from_kafka(self) -> DataFrame:
        """Чтение данных из Kafka топика"""
        logger.info(f"Подключение к Kafka: {self.kafka_broker}, топик: {self.kafka_topic}")
        
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_broker) \
            .option("subscribe", self.kafka_topic) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", "1000") \
            .load()
        
        return df.selectExpr("CAST(value AS STRING) as json", 
                            "CAST(timestamp AS TIMESTAMP) as kafka_timestamp")
    
    def parse_json_data(self, df: DataFrame) -> Tuple[DataFrame, DataFrame, DataFrame]:
        """Парсинг JSON данных с определением типа"""
        # Создаем общую схему для начального парсинга
        common_schema = StructType([
            StructField("data_type", StringType(), True),
            StructField("batch_id", StringType(), True)
        ])

        # Сначала парсим только для определения типа данных
        parsed_df = df.select(
            from_json(col("json"), common_schema).alias("metadata"),
            col("json"),
            col("kafka_timestamp")
        ).select(
            col("metadata.data_type").alias("data_type"),
            col("metadata.batch_id").alias("batch_id"),
            col("json"),
            col("kafka_timestamp")
        )

        # Разделяем по типам данных
        transaction_df = parsed_df.filter(col("data_type") == "transaction") \
            .select(from_json(col("json"), self.schemas["transaction"]).alias("data")) \
            .select("data.*")

        activity_df = parsed_df.filter(col("data_type") == "activity") \
            .select(from_json(col("json"), self.schemas["activity"]).alias("data")) \
            .select("data.*")

        sensor_df = parsed_df.filter(col("data_type") == "sensor") \
            .select(from_json(col("json"), self.schemas["sensor"]).alias("data")) \
            .select("data.*")

        return transaction_df, activity_df, sensor_df
    
    def process_transactions(self, transaction_df: DataFrame) -> List[DataFrame]:
        """Обработка транзакционных данных"""
        # Основные агрегации по часам
        hourly_stats = transaction_df \
            .withWatermark("transaction_date", "1 hour") \
            .groupBy(
                window(col("transaction_date"), "1 hour").alias("window"),
                col("user_city"),
                col("transaction_category")
            ) \
            .agg(
                count("*").alias("transaction_count"),
                sum("transaction_amount").alias("total_amount"),
                avg("transaction_amount").alias("avg_amount"),
                min("transaction_amount").alias("min_amount"),
                max("transaction_amount").alias("max_amount")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("user_city"),
                col("transaction_category"),
                col("transaction_count"),
                col("total_amount"),
                col("avg_amount"),
                col("min_amount"),
                col("max_amount")
            )
        
        # Топ пользователей по тратам
        top_users = transaction_df \
            .withWatermark("transaction_date", "1 hour") \
            .groupBy(
                window(col("transaction_date"), "1 hour").alias("window"),
                col("user_id"),
                col("user_name"),
                col("user_city")
            ) \
            .agg(
                sum("transaction_amount").alias("total_spent"),
                count("*").alias("transaction_count")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("user_id"),
                col("user_name"),
                col("user_city"),
                col("total_spent"),
                col("transaction_count")
            )
        
        # Статистика по методам оплаты
        payment_stats = transaction_df \
            .withWatermark("transaction_date", "30 minutes") \
            .groupBy(
                window(col("transaction_date"), "30 minutes").alias("window"),
                col("payment_method")
            ) \
            .agg(
                count("*").alias("transaction_count"),
                sum("transaction_amount").alias("total_amount"),
                avg("transaction_amount").alias("avg_amount")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("payment_method"),
                col("transaction_count"),
                col("total_amount"),
                col("avg_amount")
            )
        
        # Статистика по устройствам
        device_stats = transaction_df \
            .withWatermark("transaction_date", "30 minutes") \
            .groupBy(
                window(col("transaction_date"), "30 minutes").alias("window"),
                col("device").alias("device_type")
            ) \
            .agg(
                count("*").alias("transaction_count"),
                (sum(when(col("status") == "success", 1).otherwise(0)) / count("*") * 100).alias("success_rate")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("device_type"),
                col("transaction_count"),
                col("success_rate")
            )
        
        return [hourly_stats, top_users, payment_stats, device_stats]
    
    def process_activities(self, activity_df: DataFrame) -> DataFrame:
        """Обработка данных активности пользователей"""
        activity_stats = activity_df \
            .withWatermark("timestamp", "30 minutes") \
            .groupBy(
                window(col("timestamp"), "30 minutes").alias("window"),
                col("activity_type"),
                col("location.city").alias("city")
            ) \
            .agg(
                count("*").alias("activity_count"),
                avg("session_duration").alias("avg_session_duration")
            ) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("activity_type"),
                col("city"),
                col("activity_count"),
                col("avg_session_duration")
            )
        
        return activity_stats
    
    def process_sensors(self, sensor_df: DataFrame) -> DataFrame:
        """Обработка данных датчиков IoT"""
        sensor_anomalies = sensor_df \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                window(col("timestamp"), "10 minutes").alias("window"),
                col("sensor_id"),
                col("sensor_type"),
                col("sensor_location"),
                col("status")
            ) \
            .agg(
                count("*").alias("reading_count"),
                avg("value").alias("avg_value"),
                stddev("value").alias("std_value")
            ) \
            .filter(col("status").isin(["warning", "critical"])) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("sensor_id"),
                col("sensor_type"),
                col("sensor_location"),
                col("status"),
                col("reading_count"),
                col("avg_value"),
                col("std_value")
            )
        
        return sensor_anomalies
    
    def calculate_hourly_metrics(self,
                                transaction_df: DataFrame,
                                activity_df: DataFrame,
                                sensor_df: DataFrame) -> DataFrame:
        """Расчет сводных метрик по часам"""
        # Агрегация транзакций по часам
        hourly_metrics = transaction_df \
            .withWatermark("transaction_date", "1 hour") \
            .groupBy(
                window(col("transaction_date"), "1 hour").alias("window")
            ) \
            .agg(
                count("*").alias("total_transactions"),
                sum("transaction_amount").alias("total_amount"),
                avg("transaction_amount").alias("avg_transaction_amount"),
                approx_count_distinct("user_id").alias("active_users")
            ) \
            .select(
                col("window.start").alias("hour"),
                col("total_transactions"),
                col("total_amount"),
                col("avg_transaction_amount"),
                col("active_users"),
                lit(0).alias("anomaly_count")  # Временно устанавливаем 0, аномалии можно добавить отдельно
            )

        return hourly_metrics
    
    def write_to_postgres(self, df: DataFrame, table_name: str, mode: str = "append"):
        """Запись DataFrame в PostgreSQL"""
        query = df.writeStream \
            .foreachBatch(lambda batch_df, batch_id:
                self._write_batch_to_postgres(batch_df, batch_id, table_name, mode)) \
            .outputMode(mode) \
            .trigger(processingTime="30 seconds") \
            .start()

        return query
    
    def _write_batch_to_postgres(self, batch_df, batch_id, table_name, mode):
        """Запись батча данных в PostgreSQL"""
        try:
            # Кэшируем данные для избежания повторных вычислений
            batch_df.persist()

            # Debug: показать количество записей в батче
            count = batch_df.count()
            logger.info(f"Получен батч для таблицы {table_name}: {count} строк, batch_id: {batch_id}")

            if count > 0:
                # Показать первые несколько записей для отладки
                batch_df.show(5, truncate=False)

                # Запись в PostgreSQL
                batch_df.write \
                    .format("jdbc") \
                    .option("url", self.postgres_url) \
                    .option("dbtable", table_name) \
                    .option("user", self.postgres_user) \
                    .option("password", self.postgres_password) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode(mode) \
                    .save()

                logger.info(f"Записано {count} строк в таблицу {table_name}, batch_id: {batch_id}")
            else:
                logger.warning(f"Батч для таблицы {table_name} пустой, пропускаем запись")

            # Освобождаем кэш
            batch_df.unpersist()
            
        except Exception as e:
            logger.error(f"Ошибка при записи в PostgreSQL (таблица: {table_name}): {e}")
            # В случае ошибки пытаемся записать в резервное хранилище
            self._write_to_backup(batch_df, table_name, batch_id, str(e))
    
    def _write_to_backup(self, batch_df, table_name, batch_id, error_msg):
        """Запись в резервное хранилище в случае ошибки"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = f"/tmp/backup/{table_name}/{timestamp}_{batch_id}.parquet"
            
            batch_df.write \
                .mode("overwrite") \
                .parquet(backup_path)
            
            logger.warning(f"Данные сохранены в резервное хранилище: {backup_path}")
            
        except Exception as backup_error:
            logger.error(f"Ошибка при записи в резервное хранилище: {backup_error}")
    
    def start_streaming(self):
        """Запуск потоковой обработки"""
        logger.info("=" * 60)
        logger.info("Запуск Spark Streaming Consumer")
        logger.info(f"Spark Master: {self.spark_master}")
        logger.info(f"Kafka Broker: {self.kafka_broker}")
        logger.info(f"PostgreSQL: {self.postgres_url}")
        logger.info("=" * 60)

        try:
            # Создание таблиц в PostgreSQL
            logger.info("Создание таблиц в PostgreSQL...")
            self._create_postgres_tables()

            # 1. Чтение данных из Kafka
            kafka_df = self.read_from_kafka()
            
            # 2. Парсинг и разделение данных
            transaction_df, activity_df, sensor_df = self.parse_json_data(kafka_df)
            
            # 3. Обработка разных типов данных
            logger.info("Обработка транзакционных данных...")
            transaction_results = self.process_transactions(transaction_df)
            
            logger.info("Обработка данных активности...")
            activity_results = self.process_activities(activity_df)
            
            logger.info("Обработка данных датчиков...")
            sensor_results = self.process_sensors(sensor_df)
            
            logger.info("Расчет сводных метрик...")
            hourly_metrics = self.calculate_hourly_metrics(
                transaction_df, activity_df, sensor_df
            )
            
            # 4. Запуск записи в PostgreSQL
            logger.info("Запуск записи в PostgreSQL...")
            
            queries = []
            
            # Таблицы для транзакций
            transaction_tables = [
                "transaction_hourly_stats",
                "top_users", 
                "payment_method_stats",
                "device_stats"
            ]
            
            for i, result_df in enumerate(transaction_results):
                query = self.write_to_postgres(result_df, transaction_tables[i])
                queries.append(query)
                logger.info(f"Запись запущена для таблицы: {transaction_tables[i]}")
            
            # Таблицы для активности и датчиков
            query_activity = self.write_to_postgres(activity_results, "activity_stats")
            queries.append(query_activity)
            logger.info("Запись запущена для таблицы: activity_stats")
            
            query_sensor = self.write_to_postgres(sensor_results, "sensor_anomalies")
            queries.append(query_sensor)
            logger.info("Запись запущена для таблицы: sensor_anomalies")
            
            query_metrics = self.write_to_postgres(hourly_metrics, "hourly_metrics")
            queries.append(query_metrics)
            logger.info("Запись запущена для таблицы: hourly_metrics")
            
            # 5. Ожидание завершения всех запросов
            logger.info("Все запросы запущены. Ожидание завершения...")
            
            for query in queries:
                query.awaitTermination()
            
        except KeyboardInterrupt:
            logger.info("Получен сигнал прерывания. Остановка...")
        except Exception as e:
            logger.error(f"Критическая ошибка в потоковой обработке: {e}")
            raise
        finally:
            # Очистка ресурсов
            self.spark.stop()
            logger.info("Spark сессия остановлена")

def main():
    """Основная функция запуска"""
    try:
        consumer = BigDataStreamingConsumer()
        consumer.start_streaming()
    except Exception as e:
        logger.error(f"Ошибка при запуске consumer: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()