from src.pipeline import CryptoDataPipeline
from src.config import *
import time

def main():
    try:
        # Initialize the pipeline
        pipeline = CryptoDataPipeline(
            coinbase_api_key=COINBASE_API_KEY,
            coinbase_api_secret=COINBASE_API_SECRET,
            kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            influxdb_url=INFLUXDB_URL,
            influxdb_token=INFLUXDB_TOKEN,
            influxdb_org=INFLUXDB_ORG,
            influxdb_bucket=INFLUXDB_BUCKET
        )

        print("Starting Crypto Data Pipeline...")
        print(f"Tracking pairs: {CRYPTO_PAIRS}")
        print(f"Kafka Topic: {KAFKA_TOPIC}")
        
        # Run the pipeline
        pipeline.run_pipeline(CRYPTO_PAIRS, KAFKA_TOPIC)

    except KeyboardInterrupt:
        print("\nStopping pipeline...")
    except Exception as e:
        print(f"Error in pipeline: {e}")

if __name__ == "__main__":
    main()