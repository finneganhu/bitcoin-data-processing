# Importing modules
import argparse
import atexit
import happybase
import json
import logging
import time

from kafka import KafkaProducer

# Configuring logger
logger_format = '%(asctime)s - %(message)s'
logging.basicConfig(format = logger_format)
logger = logging.getLogger('data-producer')
logger.setLevel(logging.DEBUG)

# Function to set up shutdown hook called before shutdown
def shutdown_hook(producer, connection):
    """
    a shutdown hook to be called before the shutdown
    :param producer: instance of a kafka producer
    :param connection: instance of a hbase connection
    :return: None
    """
    try:
        logger.info('Closing Kafka producer')
        producer.flush(10)
        producer.close()
        logger.info('Kafka producer closed')
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

    # Initiating a simple kafka producer
    kafka_producer = KafkaProducer(bootstrap_servers = kafka_broker)

    # Initiating a hbase connection
    hbase_connection = happybase.Connection(hbase_host)

    # Setting up proper shutdown hook
    atexit.register(shutdown_hook, kafka_producer, hbase_connection)

    # Exit if the table is not found
    hbase_tables = [table.decode() for table in hbase_connection.tables()]
    if data_table not in hbase_tables:
        exit()

    # Scanning the table and pushing to kafka
    table = hbase_connection.table(data_table)

    for key, data in table.scan():
        payload = {
            'Symbol': data[b'family:symbol'].decode(),
            'LastTradePrice': data[b'family:price'].decode(),
            'Timestamp': data[b'family:time'].decode()
        }
        logger.debug('Read data from hbase: %s' % payload)
        kafka_producer.send(
            topic_name,
            value = json.dumps(payload).encode('utf-8')
        )
        time.sleep(1)
