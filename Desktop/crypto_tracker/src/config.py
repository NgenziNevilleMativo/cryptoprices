import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Coinbase settings
COINBASE_API_KEY = os.getenv('COINBASE_API_KEY')
COINBASE_API_SECRET = os.getenv('COINBASE_API_SECRET')

# Kafka settings
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = 'crypto-prices'

# InfluxDB settings
INFLUXDB_URL = 'http://localhost:8086'
INFLUXDB_TOKEN = os.getenv('INFLUXDB_TOKEN')
INFLUXDB_ORG = 'your-org'
INFLUXDB_BUCKET = 'crypto-prices'

# Crypto pairs to track
CRYPTO_PAIRS = ['BTC-USD', 'ETH-USD', 'SOL-USD']