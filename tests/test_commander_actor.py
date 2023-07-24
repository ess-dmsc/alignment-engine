import threading
import json
import time
import unittest
from unittest.mock import MagicMock, call

import pykka
import pytest
from confluent_kafka import KafkaException

from src.main.actors.commander_actor import CommanderActor, CommanderLogic
from tests.doubles.consumer import ConsumerStub

COMMAND_TOPIC = 'command_topic'
BROKER = 'localhost:9092'

CONFIG = {
    'device1': {
        'topic': 'device1_topic',
        'source': 'device1_source',
    },
    'device2': {
        'topic': 'device2_topic',
        'source': 'device2_source',
    },
}


class TestCommanderActor:
    @pytest.fixture
    def setup(self):
        self.state_machine_actor_mock = MagicMock()
        self.commander_mock = MagicMock(spec=CommanderLogic)
        self.actor_ref = CommanderActor.start(self.state_machine_actor_mock, self.commander_mock)
        self.actor_proxy = self.actor_ref.proxy()
        yield
        pykka.ActorRegistry.stop_all()

    def test_on_start_fetches_commands(self, setup):
        self.actor_ref.tell({'command': 'START'})
        time.sleep(0.01)
        self.commander_mock.consume_message.assert_called_once()

    def test_on_command_received_tells_self(self, setup):
        with unittest.mock.patch.object(self.actor_ref, 'tell') as mock_tell:
            self.actor_proxy.on_data_received({'command': 'START'}).get()
            mock_tell.assert_called_once_with({'command': 'START'})

    def test_stop_stops_commander_and_super(self, setup):
        self.actor_proxy.stop().get()
        assert self.commander_mock.stop.called

    def test_on_receive_without_command_key(self, setup):
        self.actor_proxy.on_receive({'not_a_command': 'START'}).get()
        self.state_machine_actor_mock.on_receive.assert_not_called()

    def test_on_receive_with_unexpected_command_value(self, setup):
        self.actor_proxy.on_receive({'command': 'unexpected_value'}).get()
        self.state_machine_actor_mock.on_receive.assert_not_called()


def test_fetch_command():
    consumer_stub = ConsumerStub({})
    consumer_stub.add_message('topic', 0, 1, None, '{"command": "START"}'.encode('utf-8'))
    consumer_stub.add_message('topic', 0, 2, None, '{"command": "STOP"}'.encode('utf-8'))

    received_data = []

    def mock_callback(fake_data):
        command = fake_data.get('command', None)
        if command:
            return
        received_data.append(fake_data)

    commander_logic = CommanderLogic(consumer_stub, mock_callback)

    for i in range(2):
        commander_logic.consume_message()

    assert json.loads(received_data[0]['data']) == {"command": "START"}
    assert json.loads(received_data[1]['data']) == {"command": "STOP"}


def test_can_send_config_command():
    consumer_stub = ConsumerStub({})
    consumer_stub.add_message(
        'topic', 0, 1, None,
        ('{"command": "config", "config": ' + json.dumps(CONFIG) + '}').encode('utf-8')
    )

    received_data = []

    def mock_callback(fake_data):
        command = fake_data.get('command', None)
        if command:
            return
        received_data.append(fake_data)

    commander_logic = CommanderLogic(consumer_stub, mock_callback)

    commander_logic.consume_message()

    assert json.loads(received_data[0]['data']) == {"command": "config", "config": CONFIG}


def test_fetch_commands_with_kafka_exception():
    consumer_stub = ConsumerStub({})
    consumer_stub.add_message('topic', 0, 1, None, None, KafkaException("Kafka error"))
    consumer_stub.add_message('topic', 0, 1, None, '{"command": "START"}'.encode('utf-8'), KafkaException("Kafka error"))

    received_data = []

    def mock_callback(fake_data):
        command = fake_data.get('command', None)
        if command:
            return
        received_data.append(fake_data)

    commander_logic = CommanderLogic(consumer_stub, mock_callback)

    for i in range(2):
        commander_logic.consume_message()

    assert not received_data


def test_fetch_commands_with_indecodable_message():
    consumer_stub = ConsumerStub({})
    consumer_stub.add_message('topic', 0, 1, None, b'\x80', None)  # un-decodable message

    received_data = []

    def mock_callback(fake_data):
        command = fake_data.get('command', None)
        if command:
            return
        received_data.append(fake_data)

    commander_logic = CommanderLogic(consumer_stub, mock_callback)

    commander_logic.consume_message()

    assert not received_data


def test_with_null_message_value():
    consumer_stub = ConsumerStub({})
    consumer_stub.add_message('topic', 0, 1, None, None)  # Null value

    received_data = []

    def mock_callback(fake_data):
        command = fake_data.get('command', None)
        if command:
            return
        received_data.append(fake_data)

    commander_logic = CommanderLogic(consumer_stub, mock_callback)

    commander_logic.consume_message()

    assert not received_data
