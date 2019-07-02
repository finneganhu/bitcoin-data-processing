# Importing modules
import argparse

from kafka import KafkaConsumer

# Setting default parameters
topic_name = 'test'
kafka_broker = '127.0.0.1:9092'

# Function to setup kafka consumer
def consume(topic_name, kafka_broker):
    # Consuming latest messages and auto-commit offsets
    consumer = KafkaConsumer(topic_name, bootstrap_servers = kafka_broker)

    # Just print the messages for now
    for message in consumer:
        print (message)

# 'main method'
if __name__ == '__main__':
    # Setting up comman line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('topic_name', help = 'the kafka topic to push to')
    parser.add_argument('kafka_broker', help = 'the location of the kafka broker')

    # Parsing user input arguments
    args = parser.parse_args()
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker

    consume(topic_name, kafka_broker)
