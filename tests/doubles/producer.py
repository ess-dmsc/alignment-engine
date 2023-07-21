from confluent_kafka import KafkaException


class ProducerSpy:
    def __init__(self, conf, should_throw=False):
        self.conf = conf
        self.data = []
        self.should_throw = should_throw

    def produce(self, topic, value=None, key=None, headers=None, timestamp=None, callback=None):
        if self.should_throw:
            raise KafkaException()
        self.data.append({"topic": topic, "value": value, "key": key, "headers": headers, "timestamp": timestamp})

    def flush(self, timeout=None):
        return True
