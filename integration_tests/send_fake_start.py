from confluent_kafka import Producer

START_JSON = b"""
{
"command": "START"
}
"""

STOP_JSON = b"""
{
"command": "STOP"
}
"""


def produce_start():
    producer = Producer({"bootstrap.servers": 'localhost:9092', "message.max.bytes": 100_000_000})
    producer.produce("alien_commands", START_JSON)
    producer.flush()


def produce_stop():
    producer = Producer({"bootstrap.servers": 'localhost:9092', "message.max.bytes": 100_000_000})
    producer.produce("alien_commands", STOP_JSON)
    producer.flush()


if __name__ == "__main__":
    produce_start()