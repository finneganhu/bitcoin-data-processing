# Importing modules
import argparse
import atexit
import happybase
import json
import logging

from kafka import KafkaConsumer

# Configuring logger
logger_format = '%(asctime)s - %(message)s'
logging.basicConfig(format = logger_format)
logger = logging.getLogger('data-producer')
logger.setLevel(logging.DEBUG)

# Setting default parameters
topic_name = 'test'
kafka_broker = '127.0.0.1:9092'
data_table = 'test-table'
hbase_host = 'myhbase'

# Function to persist consumed message to hbase data table
def persist_data(data, hbase_connection, data_table):
    """
    helper method to persist consumed message to hbase table
    :param data: consumed data to be stored
    :param hbase_connection: instance of hbase connection
    :param data_table: hbase data table to store the data
    :return: None
    """
    try:
        logger.debug('Starting to persist data to hbase: %s' % data)
        parsed = json.loads(data)
        symbol = parsed.get('Symbol')
        price = float(parsed.get('LastTradePrice'))
        timestamp = parsed.get('Timestamp')

        table = hbase_connection.table(data_table)
        row_key = "%s-%s" % (symbol, timestamp)
        logger.info('Storing values with row key %s' % row_key)
        table.put(row_key, {
            'family:symbol': str(symbol),
            'family:timestamp': str(timestamp),
            'family:price': str(price)
        })
        logger.info('Persisted data to hbase for symbol: %s, price: %f, timestamp: %s' %
            (symbol, price, timestamp)
        )
    except Exception as e:
        logger.error('Failed to persist data to hbase for %s' % str(e))

# Function to set up shutdown hook called before shutdown
def shutdown_hook(consumer, connection):
    """
    a shutdown hook to be called before the shutdown
    :param consumer: instance of a kafka consumer
    :param connection: instance of a hbase connection
    :return: None
    """
    try:
        logger.info('Closing Kafka consumer')
        consumer.close()
        logger.info('Kafka consumer closed')
        logger.info('Closing Hbase connection')
        connection.close()
        logger.info('Hbase connection closed')
    except Exception as e:
        logger.warn('Failed to close consumer/connection, caused by: %s' % str(e))
    finally:
        logger.info('Exiting program')


# 'main method'
if __name__ == '__main__':
    # Setting up comman line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('topic_name', help = 'the kafka topic to push to')
    parser.add_argument('kafka_broker', help = 'the location of the kafka broker')
    parser.add_argument('data_table', help = 'the data table to use')
    parser.add_argument('hbase_host', help = 'the host name of hbase')

    # Parsing user input arguments
    args = parser.parse_args()
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker
    data_table = args.data_table
    hbase_host = args.hbase_host

    # Initiating a simple kafka consumer
    consumer = KafkaConsumer(topic_name, bootstrap_servers = kafka_broker)

    # Initiating a hbase connection
    hbase_connection = happybase.Connection(hbase_host)

    # Creating the table if not already exists
    if data_table not in hbase_connection.tables():
        hbase_connection.create_table(
            data_table,
            { 'family': dict() }
        )

    # Setting up proper shutdown hook
    atexit.register(shutdown_hook, consumer, hbase_connection)

    # Storing consumed message to hbase data table
    for msg in consumer:
        persist_data(msg.value, hbase_connection, data_table)
