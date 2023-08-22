from confluent_kafka import Producer

CONFIG_JSON = b"""
{
"command": "CONFIG", 
"config":
    {
        "stream_configs": {
            "device1": {
                "broker": "localhost:9092",
                "topic": "motor_data",
                "source": "f144_source_1",
                "is_control": true
            },
            "device2": {
                "broker": "localhost:9092",
                "topic": "event_data",
                "source": "ev44_source_1",
                "is_control": false
            }
        },
        "fitter_config": {
            "control_signals": ["f144_source_1"],
            "readout_signals": ["ev44_source_1"],
            "fit_function": "gauss"
        }
    }
}
"""


def produce_config():
    producer = Producer({"bootstrap.servers": 'localhost:9092', "message.max.bytes": 100_000_000})
    producer.produce("alien_commands", CONFIG_JSON)
    producer.flush()


if __name__ == "__main__":
    produce_config()