import uuid

from confluent_kafka import Consumer, KafkaException


class ConsumerFactory:
    def __init__(self):
        pass

    def create_consumer(self, broker, topic):
        try:
            consumer = Consumer(
                {
                    "bootstrap.servers": broker,
                    "group.id": uuid.uuid4(),
                    "auto.offset.reset": "latest",
                }
            )
            consumer.subscribe([topic])
            metadata = consumer.list_topics(topic=topic)

            return consumer
        except KafkaException as e:
            print(f"Failed to create consumer on broker {broker} and subscribe to topic {topic}")
            raise e

