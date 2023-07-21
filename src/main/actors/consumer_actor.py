import pykka
from confluent_kafka import KafkaException

from streaming_data_types import deserialise_ev44, deserialise_f144


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
        self.consumer_logic.set_callback(self.on_data_received)
        self.status = 'IDLE'

    def on_start(self):
        print(f"Starting {self.__class__.__name__}")
        self.consumer_supervisor.tell({'command': 'REGISTER', 'actor': self.actor_ref})
        self.status = 'RUNNING'

    def on_failure(self, exception_type, exception_value, traceback):
        self.consumer_supervisor.tell({'command': 'FAILED', 'actor': self.actor_ref})

    def on_receive(self, message):
        if not isinstance(message, dict):
            print(f"Unknown message: {message}")
            return

        command = message.get('command', None)

        if command is not None:
            if command == 'START':
                self.status = 'RUNNING'
                self.actor_ref.tell({'command': 'CONSUME'})
            elif command == 'STOP':
                self.stop()
            elif command == 'CONSUME':
                self.consumer_logic.consume_message()
            elif command == 'STATUS':
                return self.get_status()
            return

        data = message.get('data', None)
        if data is not None:
            self.data_handler_actor.tell(message)
            return
        else:
            print(f"Unknown message: {message}")

    def on_data_received(self, data):
        self.actor_ref.tell(data)

    def get_status(self):
        return self.status

    def stop(self):
        self.consumer_logic.stop()
        super().stop()


class ConsumerLogic:
    def __init__(self, consumer, callback=None, source_name=None):
        self.consumer = consumer
        self.callback = callback
        self.running = True
        self.source_name = source_name

    def consume_message(self):
        msg = self._poll_message()
        if msg is not None:
            self._process_message(msg)
        if self.running:
            self.callback({'command': 'CONSUME'})

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
            self.callback({'data': decoded_message})
        except UnicodeDecodeError:
            print("Cannot deserialise message")

    def set_callback(self, callback):
        self.callback = callback

    def stop(self):
        self.running = False
        self.consumer.close()
