import os
import time
import json
import yfinance as yf
from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TICKER_SYMBOL = os.environ.get('TICKER_SYMBOL', 'BTC-USD')
TOPIC_NAME = 'ticker_prices'

def create_producer():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except Exception as e:
            print(f"Failed to connect to Kafka: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def fetch_price(ticker):
    try:
        data = yf.Ticker(ticker)

        price = data.fast_info['last_price']
        return price
    except Exception as e:
        print(f"Error fetching price for {ticker}: {e}")
        return None

def main():
    producer = create_producer()
    
    print(f"Starting producer for {TICKER_SYMBOL}...")
    
    while True:
        price = fetch_price(TICKER_SYMBOL)
        if price:
            timestamp = int(time.time() * 1000)
            payload = {
                'symbol': TICKER_SYMBOL,
                'price': price,
                'timestamp': timestamp
            }
            try:
                producer.send(TOPIC_NAME, payload)
                print(f"Sent: {payload}")
            except Exception as e:
                print(f"Error sending to Kafka: {e}")
        
        time.sleep(5)

if __name__ == "__main__":
    main()
