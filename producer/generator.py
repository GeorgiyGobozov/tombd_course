import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from faker import Faker
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataGenerator:
    def __init__(self, kafka_broker='kafka:9092', topic='user_transactions'):
        self.faker = Faker('ru_RU')
        self.topic = topic

        # Retry logic for Kafka connection
        max_retries = 10
        retry_delay = 5
        for attempt in range(max_retries):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=kafka_broker,
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                    acks='all',
                    retries=3
                )
                logger.info(f"Successfully connected to Kafka broker: {kafka_broker}")
                break
            except NoBrokersAvailable:
                if attempt < max_retries - 1:
                    logger.warning(f"Failed to connect to Kafka broker (attempt {attempt + 1}/{max_retries}). Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"Failed to connect to Kafka broker after {max_retries} attempts.")
                    raise
        
        # Список городов для генерации данных
        self.cities = ['Москва', 'Санкт-Петербург', 'Новосибирск', 'Екатеринбург', 'Казань',
                      'Нижний Новгород', 'Челябинск', 'Самара', 'Омск', 'Ростов-на-Дону']
        
        # Категории товаров
        self.categories = ['Электроника', 'Одежда', 'Продукты', 'Книги', 'Спорттовары',
                          'Красота', 'Дом', 'Автотовары', 'Зоотовары', 'Игрушки']
        
        # Список предопределенных пользователей
        self.users = []
        for _ in range(100):
            self.users.append({
                'id': self.faker.uuid4(),
                'name': self.faker.name(),
                'email': self.faker.email(),
                'city': random.choice(self.cities),
                'age': random.randint(18, 70),
                'registration_date': self.faker.date_between(start_date='-5y', end_date='today').isoformat()
            })
        
        logger.info(f"Инициализирован генератор данных для {len(self.users)} пользователей")

    def generate_transaction(self):
        """Генерация одной транзакции"""
        user = random.choice(self.users)
        transaction_id = self.faker.uuid4()
        amount = round(random.uniform(100, 50000), 2)
        category = random.choice(self.categories)
        
        transaction = {
            'transaction_id': transaction_id,
            'user_id': user['id'],
            'user_name': user['name'],
            'user_city': user['city'],
            'user_age': user['age'],
            'user_email': user['email'],
            'transaction_amount': amount,
            'transaction_currency': 'RUB',
            'transaction_category': category,
            'merchant': self.faker.company(),
            'transaction_date': datetime.now().isoformat(),
            'payment_method': random.choice(['credit_card', 'debit_card', 'digital_wallet', 'cash']),
            'status': random.choices(['success', 'pending', 'failed'], weights=[0.9, 0.07, 0.03])[0],
            'device': random.choice(['mobile', 'desktop', 'tablet']),
            'ip_address': self.faker.ipv4(),
            'metadata': {
                'items_count': random.randint(1, 10),
                'discount_applied': random.choice([True, False]),
                'loyalty_points_earned': int(amount / 100)
            }
        }
        return transaction

    def generate_user_activity(self):
        """Генерация данных о активности пользователя"""
        user = random.choice(self.users)
        
        activity = {
            'activity_id': self.faker.uuid4(),
            'user_id': user['id'],
            'user_name': user['name'],
            'activity_type': random.choice(['login', 'logout', 'page_view', 'search', 'add_to_cart', 'purchase']),
            'page_url': self.faker.uri(),
            'session_duration': random.randint(10, 3600),
            'timestamp': datetime.now().isoformat(),
            'device_info': {
                'browser': random.choice(['Chrome', 'Firefox', 'Safari', 'Edge']),
                'os': random.choice(['Windows', 'macOS', 'Linux', 'Android', 'iOS']),
                'screen_resolution': f"{random.randint(800, 3840)}x{random.randint(600, 2160)}"
            },
            'location': {
                'city': user['city'],
                'country': 'Russia',
                'timezone': 'Europe/Moscow'
            }
        }
        return activity

    def generate_sensor_data(self):
        """Генерация данных IoT-датчиков"""
        sensor_types = ['temperature', 'humidity', 'pressure', 'vibration', 'power_consumption']
        sensor_type = random.choice(sensor_types)
        
        sensor_data = {
            'sensor_id': f"sensor_{random.randint(1, 1000):04d}",
            'sensor_type': sensor_type,
            'sensor_location': random.choice(['factory_floor', 'warehouse', 'office', 'server_room']),
            'timestamp': datetime.now().isoformat(),
            'value': self._generate_sensor_value(sensor_type),
            'unit': self._get_sensor_unit(sensor_type),
            'status': random.choices(['normal', 'warning', 'critical'], weights=[0.85, 0.1, 0.05])[0],
            'battery_level': random.randint(10, 100),
            'signal_strength': random.randint(1, 5)
        }
        return sensor_data

    def _generate_sensor_value(self, sensor_type):
        if sensor_type == 'temperature':
            return round(random.uniform(-10, 40), 1)
        elif sensor_type == 'humidity':
            return random.randint(30, 90)
        elif sensor_type == 'pressure':
            return round(random.uniform(950, 1050), 1)
        elif sensor_type == 'vibration':
            return round(random.uniform(0, 10), 3)
        else:  # power_consumption
            return round(random.uniform(0, 1000), 2)

    def _get_sensor_unit(self, sensor_type):
        units = {
            'temperature': '°C',
            'humidity': '%',
            'pressure': 'hPa',
            'vibration': 'm/s²',
            'power_consumption': 'W'
        }
        return units.get(sensor_type, 'unit')

    def start_generation(self, interval=0.5):
        """Запуск генерации данных"""
        logger.info(f"Начало генерации данных. Топик: {self.topic}")
        
        message_count = 0
        try:
            while True:
                # Генерация разных типов данных с разной вероятностью
                data_type = random.choices(
                    ['transaction', 'activity', 'sensor'],
                    weights=[0.6, 0.3, 0.1]
                )[0]
                
                if data_type == 'transaction':
                    data = self.generate_transaction()
                elif data_type == 'activity':
                    data = self.generate_user_activity()
                else:
                    data = self.generate_sensor_data()
                
                # Добавление метаданных
                data['data_type'] = data_type
                data['batch_id'] = f"batch_{datetime.now().strftime('%Y%m%d_%H')}"
                
                # Отправка в Kafka
                self.producer.send(self.topic, data)
                message_count += 1
                
                if message_count % 100 == 0:
                    logger.info(f"Отправлено сообщений: {message_count}")
                
                # Небольшая задержка для реалистичности
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("Остановка генератора данных...")
        except Exception as e:
            logger.error(f"Ошибка при генерации данных: {e}")
        finally:
            self.producer.flush()
            self.producer.close()
            logger.info(f"Всего отправлено сообщений: {message_count}")

if __name__ == "__main__":
    import os
    
    # Получение настроек из переменных окружения
    kafka_broker = os.getenv('KAFKA_BROKER', 'kafka:9092')
    topic = os.getenv('KAFKA_TOPIC', 'user_transactions')
    interval = float(os.getenv('GENERATION_INTERVAL', 0.5))
    
    # Создание и запуск генератора
    generator = DataGenerator(kafka_broker, topic)
    generator.start_generation(interval)