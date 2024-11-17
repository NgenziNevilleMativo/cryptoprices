import json
import time
from datetime import datetime
import pandas as pd
from coinbase.wallet.client import Client
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

class CryptoDataPipeline:
    def __init__(self, coinbase_api_key, coinbase_api_secret, kafka_bootstrap_servers,
                 influxdb_url, influxdb_token, influxdb_org, influxdb_bucket):
        # Initialize Coinbase client
        self.coinbase_client = Client(coinbase_api_key, coinbase_api_secret)
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        # Initialize InfluxDB client
        self.influxdb_client = InfluxDBClient(
            url=influxdb_url,
            token=influxdb_token,
            org=influxdb_org
        )
        self.write_api = self.influxdb_client.write_api(write_options=SYNCHRONOUS)
        self.bucket = influxdb_bucket
        self.org = influxdb_org

    def get_crypto_price(self, crypto_pair):
        """Fetch current price from Coinbase"""
        try:
            price = self.coinbase_client.get_spot_price(currency_pair=crypto_pair)
            return {
                'pair': crypto_pair,
                'price': float(price.amount),
                'timestamp': datetime.utcnow().isoformat()
            }
        except Exception as e:
            print(f"Error fetching price: {e}")
            return None

    def send_to_kafka(self, topic, data):
        """Send data to Kafka topic"""
        try:
            future = self.producer.send(topic, value=data)
            self.producer.flush()
            record_metadata = future.get(timeout=10)
            print(f"Data sent to Kafka: {data}")
            return True
        except KafkaError as e:
            print(f"Error sending to Kafka: {e}")
            return False

    def calculate_moving_average(self, data_stream, window_size=20):
        """Calculate moving average from a stream of prices"""
        df = pd.DataFrame(data_stream)
        df['price'] = pd.to_numeric(df['price'])
        df['ma'] = df['price'].rolling(window=window_size).mean()
        return df

    def store_in_influxdb(self, data):
        """Store data in InfluxDB"""
        try:
            point = Point("crypto_prices") \
                .tag("pair", data['pair']) \
                .field("price", float(data['price'])) \
                .time(datetime.fromisoformat(data['timestamp']))
            
            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
            print(f"Data stored in InfluxDB: {data}")
            return True
        except Exception as e:
            print(f"Error storing in InfluxDB: {e}")
            return False

    def run_pipeline(self, crypto_pairs, kafka_topic, interval=60):
        """Run the complete pipeline"""
        while True:
            for pair in crypto_pairs:
                # Get price data
                price_data = self.get_crypto_price(pair)
                if price_data:
                    # Send to Kafka
                    self.send_to_kafka(kafka_topic, price_data)
                    # Store in InfluxDB
                    self.store_in_influxdb(price_data)
            
            time.sleep(interval)

class KafkaPriceConsumer:
    def __init__(self, kafka_bootstrap_servers, topic, window_size=20):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=kafka_bootstrap_servers,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        self.window_size = window_size
        self.price_buffer = []

    def process_stream(self):
        """Process incoming price stream and calculate moving averages"""
        for message in self.consumer:
            data = message.value
            self.price_buffer.append(data)
            
            # Keep only the last window_size elements
            if len(self.price_buffer) > self.window_size:
                self.price_buffer.pop(0)
            
            # Calculate moving average when we have enough data
            if len(self.price_buffer) == self.window_size:
                df = pd.DataFrame(self.price_buffer)
                moving_avg = df['price'].mean()
                
                print(f"Moving Average for {data['pair']}: {moving_avg}")