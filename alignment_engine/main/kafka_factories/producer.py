from confluent_kafka import Producer, KafkaException


class ProducerFactory:
    def __init__(self):
        pass

    def create_producer(self, broker):
        try:
            producer = Producer({"bootstrap.servers": broker, "message.max.bytes": 100_000_000})
            return producer
        except KafkaException as e:
            print(f"Failed to create producer on broker {broker}")
            raise e
