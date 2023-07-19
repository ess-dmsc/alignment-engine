import pykka
from confluent_kafka import KafkaException

from streaming_data_types.utils import get_schema
from streaming_data_types import deserialise_ev44, serialise_ev44, deserialise_f144, serialise_f144


deserialiser_by_schema = {
    'ev44': deserialise_ev44,
    'f144': deserialise_f144,
}


class ConsumerActor(pykka.ThreadingActor):
    def __init__(self, consumer_supervisor, data_handler_actor, consumer_logic):
        super().__init__()
        self.consumer_logic = consumer_logic
        self.consumer_supervisor = consumer_supervisor
        self.data_handler_actor = data_handler_actor
        self.status = 'IDLE'

    def on_receive(self, message):
        if message == 'START':
            self.consumer_logic.consume_messages()
            self.status = 'RUNNING'
        elif message == 'STOP':
            self.stop()
        elif isinstance(message, dict) and 'data' in message:
            self.data_handler_actor.on_receive(message)
        else:
            print(f"Unknown message: {message}")

    def on_start(self):
        self.actor_ref.proxy().consume_messages()

    def consume_messages(self):
        self.consumer_logic.consume_messages()
        self.status = 'RUNNING'

    def on_data_received(self, data):
        self.actor_ref.tell({'data': data})

    def get_status(self):
        return self.status

    def stop(self):
        self.consumer_logic.stop()
        super().stop()


class ConsumerLogic:
    def __init__(self, consumer, on_data_received, source_name=None):
        self.consumer = consumer
        self.on_data_received = on_data_received
        self.running = True
        self.source_name = source_name

    def consume_messages(self):
        while self.running:
            msg = self._poll_message()
            if msg is not None:
                self._process_message(msg)

    def _poll_message(self):
        try:
            return self.consumer.poll(timeout=0.05)
        except KafkaException as e:
            print(f"Consumer error: {e}")
            return None

    def _process_message(self, msg):
        value, error = self._parse_message(msg)
        if value is not None and error is None:
            self._decode_and_process_value(value)

    def _parse_message(self, msg):
        try:
            return msg.value(), msg.error()
        except ValueError:
            print("Cannot parse message")
            return None, None

    def _get_schema(self, value):
        try:
            return value[4:8].decode("utf-8")
        except AttributeError:
            print("Cannot decode schema type")
            return None

    def _decode_and_process_value(self, value):
        try:
            deserialiser = deserialiser_by_schema.get(self._get_schema(value), None)
            if deserialiser is None:
                print("Cannot find deserialiser")
                return
            decoded_message = deserialiser(value)
            if self.source_name is not None:
                if decoded_message.source_name != self.source_name:
                    return
            self.on_data_received({'data': decoded_message})
        except UnicodeDecodeError:
            print("Cannot deserialise message")

    def stop(self):
        self.running = False
        self.consumer.close()
