import json
import pykka
from confluent_kafka import KafkaException


ALLOWED_COMMANDS = ['start', 'stop', 'config', 'status']


class CommanderActor(pykka.ThreadingActor):
    def __init__(self, state_machine_actor, commander):
        super().__init__()
        self.commander = commander
        self.state_machine_actor = state_machine_actor
        self.status = 'IDLE'

    def on_receive(self, message):
        if message == 'START':
            self.commander.consume_messages()
            self.status = 'RUNNING'
        elif message == 'STOP':
            self.stop()
        elif 'command' in message:
            if message['command'] in ALLOWED_COMMANDS:
                self.state_machine_actor.on_receive(message)

    def on_start(self):
        self.actor_ref.proxy().fetch_commands()

    def fetch_commands(self):
        self.commander.consume_messages()
        self.status = 'RUNNING'

    def on_command_received(self, command):
        self.actor_ref.tell({'command': command})

    def get_status(self):
        return self.status

    def stop(self):
        self.commander.stop()
        super().stop()


class CommanderLogic:
    def __init__(self, consumer, on_command_received):
        self.consumer = consumer
        self.on_command_received = on_command_received
        self.running = True

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

    def _decode_and_process_value(self, value):
        try:
            decoded_message = value.decode('utf-8')
            command_message = json.loads(decoded_message)
            self.on_command_received(command_message)
        except UnicodeDecodeError:
            print("Cannot decode message")

    def stop(self):
        self.running = False
        self.consumer.close()






# try:
#     self.consumer = Consumer({'bootstrap.servers': self.broker,
#                               'group.id': 'command_group',
#                               'auto.offset.reset': 'latest'})
#
#     self.consumer.subscribe([self.command_topic])
# except KafkaException as e:
#     print(f"Kafka error: {e}")
#     return