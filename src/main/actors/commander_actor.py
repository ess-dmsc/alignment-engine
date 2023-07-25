import json

import pykka
from confluent_kafka import KafkaException


class CommanderActor(pykka.ThreadingActor):
    def __init__(self, state_machine_supervisor, commander_logic=None):
        super().__init__()
        self.commander_logic = commander_logic
        self.state_machine_supervisor = state_machine_supervisor
        if self.commander_logic is not None:
            self.commander_logic.set_callback(self.on_data_received)
        self.status = 'IDLE'

    def set_commander_logic(self, commander_logic):
        self.commander_logic = commander_logic
        if self.commander_logic is not None:
            self.commander_logic.set_callback(self.on_data_received)

    def on_start(self):
        print(f"Starting {self.__class__.__name__}")
        self.state_machine_supervisor.tell({'command': 'REGISTER', 'actor': self.actor_ref})
        self.status = 'RUNNING'

    def on_failure(self, exception_type, exception_value, traceback):
        print(f"Commander actor failed: {exception_type}, {exception_value}, {traceback}")
        self.state_machine_supervisor.tell({'command': 'FAILED', 'actor': self.actor_ref})

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
                if self.commander_logic is not None:
                    self.commander_logic.consume_message()
            elif command == 'STATUS':
                return self.get_status()
            elif command == 'SET_LOGIC':
                self.set_commander_logic(message.get('logic', None))
            return

        data = message.get('data', None)
        if data is not None:
            self.state_machine_supervisor.tell(message)
            return
        else:
            print(f"Unknown message: {message}")

    def on_data_received(self, data):
        self.actor_ref.tell(data)

    def get_status(self):
        return self.status

    def stop(self):
        if self.commander_logic is not None:
            self.commander_logic.stop()
        # super().stop()


class CommanderLogic:
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

    def _decode_and_process_value(self, value):
        try:
            decoded_message = value.decode('utf-8')
            command_message = json.loads(decoded_message)
            self.callback({'data': command_message})
        except UnicodeDecodeError:
            print("Cannot deserialise message")

    def set_callback(self, callback):
        self.callback = callback

    def stop(self):
        self.running = False
        self.consumer.close()
