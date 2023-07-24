import os
import time

import pykka
from confluent_kafka import KafkaException

from streaming_data_types import serialise_f144, serialise_x5f2


class ProducerActor(pykka.ThreadingActor):
    def __init__(self, producer_supervisor, producer_logic):
        super().__init__()
        self.producer_logic = producer_logic
        self.producer_supervisor = producer_supervisor
        self.status = 'IDLE'

    def on_start(self):
        print(f"Starting {self.__class__.__name__}")
        self.producer_supervisor.tell({'command': 'REGISTER', 'actor': self.actor_ref})
        self.status = 'RUNNING'

    def on_failure(self, exception_type, exception_value, traceback):
        self.producer_supervisor.tell({'command': 'FAILED', 'actor': self.actor_ref})

    def on_receive(self, message):
        if not isinstance(message, dict):
            print(f"Unknown message: {message}")
            return

        command = message.get('command', None)

        if command is not None:
            if command == 'START':
                self.status = 'RUNNING'
            elif command == 'STOP':
                self.stop()
            elif command == 'RESET':
                pass
            elif command == 'STATUS':
                return self.get_status()
            return

        data = message.get('data', None)
        if data is not None:
            self.producer_logic.produce_message(message)
        else:
            print(f"Unknown message: {message}")

    def on_data_received(self, data):
        self.actor_ref.tell(data)

    def get_status(self):
        return self.status

    def stop(self):
        self.producer_logic.stop()
        super().stop()


class ProducerLogic:
    def __init__(self, producer, topic, source_name=None):
        self.producer = producer
        self.topic = topic
        self.running = True
        self.source_name = source_name

    def produce_message(self, message):
        if message is not None:
            processed_messages = self._process_message(message)
            try:
                for msg in processed_messages:
                    self.producer.produce(self.topic, msg)
                    self.producer.flush()
            except KafkaException as e:
                print(f"Failed to produce message: {e}")

    def _process_message(self, message):
        data = message.get('data', None)
        if data:
            return self._process_data(data)

        status = message.get('status', None)
        if status:
            return self._process_status(status)

        return []

    def _process_data(self, data):
        processed_data = []
        t = time.time_ns()
        for name, dat in data.items():
            try:
                if 'fit_params' in dat:
                    processed_data.append(serialise_f144(name, dat['fit_params'], t))
                else:
                    processed_data.append(serialise_f144(name, dat, t))
            except Exception as e:
                print(f"Failed to serialise data: {e}")

        return processed_data

    def _process_status(self, status):
        return [serialise_x5f2(*status)]

    def stop(self):
        self.running = False
        self.producer.close()









