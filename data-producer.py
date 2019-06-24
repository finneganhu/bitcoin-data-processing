# Importing modules
import argparse
import atexit
import json
import logging
import requests
import schedule
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError

# Configuring logger
logger_format = '%(asctime)s - %(message)s'
logging.basicConfig(format = logger_format)
logger = logging.getLogger('data-producer')
logger.setLevel(logging.DEBUG)

# Defining constants
API_BASE = 'https://api.gdax.com'

# Function to check if the symbol exists in API
def check_symbol(symbol):
    """
    helper method to check if the symbol exists in coinbase API.
    """
    logger.debug('Checking symbol.')
    try:
        response = requests.get(API_BASE + '/products')
        product_ids = [product['id'] for product in response.json()]
        if symbol not in product_ids:
            logger.warning('Symbol %s not supported. The list of supported symbols: %s', symbol, product_ids)
            exit()
    except Exception as e:
        logger.warning('Failed to fetch products: %s', e)

# Function to retrieve asset data and send it to kafka
def fetch_price(symbol, producer, topic_name):
    """
    helper method to retrieve asset data and send it to kafka
    :param symbol: the symbol of asset
    :param producer: instance of kafka producer
    :param topic_name: name of the kafka topic to push to
    :return None
    """
    logger.debug('Starting to fetch price for %s', symbol)
    try:
        response = requests.get('%s/products/%s/ticker' % (API_BASE, symbol))
        price = response.json()['price']
        timestamp = time.time()
        payload = {
            'Symbol': str(symbol),
            'LastTradePrice': str(price),
            'LastTradeDateTime': str(timestamp)
        }
        logger.debug('Retrieved %s info: %s', symbol, payload)
        producer.send(
            topic = topic_name,
            value = json.dumps(payload).encode('utf-8'),
            timestamp_ms = int(time.time() * 1000)
        )
        logger.debug('Sent price for %s to Kafka', symbol)
    except KafkaTimeoutError as timeout_error:
        logger.warning('Failed to send price to kafka, caused by: %s', timeout_error.message)
    except Exception as e:
        logger.warning('Failed to fetch price: %s', e)

# Function to set up shutdown hook called before shutdown
def shutdown_hook(producer):
    """
    a shutdown hook to be called before the shutdown
    :param producer: instance of a kafka producer
    :return: None
    """
    try:
        logger.info('Flushing pending message to kafka, timeout is set to 10s')
        producer.flush(10)
        logger.info('Finish flushing pending messages to kafka')
    except KafkaError as kafka_error:
        logger.warning('Failed to flush pending messages to kafka, caused by: %s', kafka_error.message)
    finally:
        try:
            logger.info('Closing kafka connection')
            producer.close(10)
        except Exception as e:
            logger.warning('Failed to close kafka connection, caused by: %s', e.message)


# 'main method'
if __name__ == '__main__':
    # Setting up comman line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('symbol', help = 'the symbol to pull')
    parser.add_argument('topic_name', help = 'the kafka topic to push to')
    parser.add_argument('kafka_broker', help = 'the location of the kafka broker')

    # Parsing user input arguments
    args = parser.parse_args()
    symbol = args.symbol
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker

    # Checking if the inpuy symbol is supported
    check_symbol(symbol)

    # Instantiating a simple kafka producer
    producer = KafkaProducer(bootstrap_servers = kafka_broker)

    # Scheduling and running the fetch_price function per second
    schedule.every(1).second.do(fetch_price, symbol, producer, topic_name)

    # Setting up proper shutdown hook
    atexit.register(shutdown_hook, producer)

    # Running the scheduled task
    while True:
        schedule.run_pending()
        time.sleep(1)
