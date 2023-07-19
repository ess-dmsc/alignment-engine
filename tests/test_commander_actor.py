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
        self.commander_mock.consume_messages.assert_called_once()

    def test_on_receive_forwards_message_to_state_machine_actor(self, setup):
        self.actor_proxy.on_receive({'command': 'start'}).get()
        self.state_machine_actor_mock.on_receive.assert_called_once_with({'command': 'start'})

    def test_on_command_received_tells_self(self, setup):
        with unittest.mock.patch.object(self.actor_ref, 'tell') as mock_tell:
            self.actor_proxy.on_command_received('start').get()
            mock_tell.assert_called_once_with({'command': 'start'})

    def test_stop_stops_commander_and_super(self, setup):
        self.actor_proxy.stop().get()
        assert self.commander_mock.stop.called

    def test_on_receive_without_command_key(self, setup):
        self.actor_proxy.on_receive({'not_a_command': 'start'}).get()
        self.state_machine_actor_mock.on_receive.assert_not_called()

    def test_on_receive_with_unexpected_command_value(self, setup):
        self.actor_proxy.on_receive({'command': 'unexpected_value'}).get()
        self.state_machine_actor_mock.on_receive.assert_not_called()

    def test_commander_actor_concurrency(self, setup):
        thread_count = 10
        message_count = 1000

        def send_messages():
            for _ in range(message_count):
                self.actor_proxy.on_receive({'command': 'start'}).get()

        threads = [threading.Thread(target=send_messages) for _ in range(thread_count)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert self.state_machine_actor_mock.on_receive.call_count == thread_count * message_count


def test_fetch_command():
    consumer_stub = ConsumerStub({})
    consumer_stub.add_message('topic', 0, 1, None, '{"command": "start"}'.encode('utf-8'))
    consumer_stub.add_message('topic', 0, 2, None, '{"command": "stop"}'.encode('utf-8'))

    received_commands = []

    def mock_on_command_received(command):
        received_commands.append(command)

    commander_logic = CommanderLogic(consumer_stub, mock_on_command_received)

    thread = threading.Thread(target=commander_logic.consume_messages)
    thread.start()

    time.sleep(0.01)

    commander_logic.stop()
    thread.join()

    assert received_commands == [{"command": "start"}, {"command": "stop"}]


def test_can_send_config_command():
    consumer_stub = ConsumerStub({})
    consumer_stub.add_message(
        'topic', 0, 1, None,
        ('{"command": "config", "config": ' + json.dumps(CONFIG) + '}').encode('utf-8')
    )

    received_commands = []

    def mock_on_command_received(command):
        received_commands.append(command)

    commander_logic = CommanderLogic(consumer_stub, mock_on_command_received)

    thread = threading.Thread(target=commander_logic.consume_messages)
    thread.start()

    time.sleep(0.01)

    commander_logic.stop()
    thread.join()

    assert received_commands == [{"command": "config", "config": CONFIG}]


# Note: this test is not deterministic, but it is very unlikely to fail
def test_can_stop_commander():
    consumer_stub = ConsumerStub({})
    for i in range(100000):
        consumer_stub.add_message('topic', 0, i+1, None, '{"command": "start"}'.encode('utf-8'))

    received_commands = []

    def mock_on_command_received(command):
        received_commands.append(command)

    commander_logic = CommanderLogic(consumer_stub, mock_on_command_received)

    thread = threading.Thread(target=commander_logic.consume_messages)
    thread.start()

    commander_logic.stop()
    thread.join()

    assert len(received_commands) < 100000


def test_fetch_commands_with_kafka_exception():
    consumer_stub = ConsumerStub({})
    consumer_stub.add_message('topic', 0, 1, None, None, KafkaException("Kafka error"))
    consumer_stub.add_message('topic', 0, 1, None, '{"command": "start"}'.encode('utf-8'), KafkaException("Kafka error"))

    received_commands = []

    def mock_on_command_received(command):
        received_commands.append(command)

    commander_logic = CommanderLogic(consumer_stub, mock_on_command_received)

    thread = threading.Thread(target=commander_logic.consume_messages)
    thread.start()

    time.sleep(0.01)

    commander_logic.stop()
    thread.join()

    assert not received_commands


def test_fetch_commands_with_indecodable_message():
    consumer_stub = ConsumerStub({})
    consumer_stub.add_message('topic', 0, 1, None, b'\x80', None)  # un-decodable message

    received_commands = []

    def mock_on_command_received(command):
        received_commands.append(command)

    commander_logic = CommanderLogic(consumer_stub, mock_on_command_received)

    thread = threading.Thread(target=commander_logic.consume_messages)
    thread.start()

    time.sleep(0.01)

    commander_logic.stop()
    thread.join()

    assert not received_commands


def test_with_null_message_value():
    consumer_stub = ConsumerStub({})
    consumer_stub.add_message('topic', 0, 1, None, None)  # Null value

    received_commands = []

    def mock_on_command_received(command):
        received_commands.append(command)

    commander_logic = CommanderLogic(consumer_stub, mock_on_command_received)

    thread = threading.Thread(target=commander_logic.consume_messages)
    thread.start()

    time.sleep(0.01)

    commander_logic.stop()
    thread.join()

    assert not received_commands


# We probably will only run with one, but it's good to know that it works with multiple
def test_concurrent_consumers():
    consumer_stub1 = ConsumerStub({})
    consumer_stub2 = ConsumerStub({})
    for i in range(5000):
        consumer_stub1.add_message('topic', 0, i+1, None, f'{{"command": "start{i}"}}'.encode('utf-8'))
    for i in range(6000):
        consumer_stub2.add_message('topic', 0, i+1, None, f'{{"command": "start{i}"}}'.encode('utf-8'))

    received_commands1 = []
    received_commands2 = []

    def mock_on_command_received1(command):
        received_commands1.append(command)

    def mock_on_command_received2(command):
        received_commands2.append(command)

    commander_logic1 = CommanderLogic(consumer_stub1, mock_on_command_received1)
    commander_logic2 = CommanderLogic(consumer_stub2, mock_on_command_received2)

    thread1 = threading.Thread(target=commander_logic1.consume_messages)
    thread2 = threading.Thread(target=commander_logic2.consume_messages)

    thread1.start()
    thread2.start()

    time.sleep(0.1)  # Allow more time for processing

    commander_logic1.stop()
    commander_logic2.stop()

    thread1.join()
    thread2.join()

    assert len(received_commands1) == 5000
    assert len(received_commands2) == 6000










#
# @patch('src.main.actors.commander_actor.Consumer')
# @patch('src.main.actors.commander_actor.StateMachineActor.start')
# def test_start_command(mock_state_machine_start, mock_consumer):
#     mock_state_machine_proxy = MagicMock()
#     mock_state_machine_start.return_value.proxy.return_value = mock_state_machine_proxy
#
#     command_actor = CommanderActor.start(mock_state_machine_proxy, COMMAND_TOPIC, BROKER).proxy()
#
#     start_msg = MagicMock()
#     start_msg.value.return_value = json.dumps({'command': 'start'}).encode()
#     start_msg.error.return_value = None
#     mock_consumer.return_value.poll.return_value = start_msg
#
#     command_actor.fetch_command().get()
#
#     mock_state_machine_proxy.on_receive.assert_called_once_with({'command': 'start'})
#
#     command_actor.stop()
#
#
# @patch('src.main.actors.commander_actor.Consumer')
# @patch('src.main.actors.commander_actor.StateMachineActor.start')
# def test_stop_command(mock_state_machine_stop, mock_consumer):
#     mock_state_machine_proxy = MagicMock()
#     mock_state_machine_stop.return_value.proxy.return_value = mock_state_machine_proxy
#
#     command_actor = CommanderActor.start(mock_state_machine_proxy, COMMAND_TOPIC, BROKER).proxy()
#
#     stop_msg = MagicMock()
#     stop_msg.value.return_value = json.dumps({'command': 'stop'}).encode()
#     stop_msg.error.return_value = None
#     mock_consumer.return_value.poll.return_value = stop_msg
#
#     command_actor.fetch_command().get()
#
#     mock_state_machine_proxy.on_receive.assert_called_once_with({'command': 'stop'})
#
#     command_actor.stop()
#
#
# @patch('src.main.actors.commander_actor.Consumer')
# @patch('src.main.actors.commander_actor.StateMachineActor.start')
# def test_stop_command(mock_state_machine_config, mock_consumer):
#     mock_state_machine_proxy = MagicMock()
#     mock_state_machine_config.return_value.proxy.return_value = mock_state_machine_proxy
#
#     command_actor = CommanderActor.start(mock_state_machine_proxy, COMMAND_TOPIC, BROKER).proxy()
#
#
#
#     conf_msg = MagicMock()
#     conf_msg.value.return_value = json.dumps({'command': 'config', 'config': CONFIG}).encode()
#     conf_msg.error.return_value = None
#     mock_consumer.return_value.poll.return_value = conf_msg
#
#     command_actor.fetch_command().get()
#
#     mock_state_machine_proxy.on_receive.assert_called_once_with({'command': 'config', 'config': CONFIG})
#
#     command_actor.stop()
#
#
# @patch('src.main.actors.commander_actor.Consumer')
# @patch('src.main.actors.commander_actor.StateMachineActor.start')
# def test_incorrect_command(mock_state_machine_start, mock_consumer):
#     mock_state_machine_proxy = MagicMock()
#     mock_state_machine_start.return_value.proxy.return_value = mock_state_machine_proxy
#
#     command_actor = CommanderActor.start(mock_state_machine_proxy, COMMAND_TOPIC, BROKER).proxy()
#
#     incorrect_msg = MagicMock()
#     incorrect_msg.value.return_value = json.dumps({'command': 'incorrect'}).encode()
#     incorrect_msg.error.return_value = None
#     mock_consumer.return_value.poll.return_value = incorrect_msg
#
#     assert command_actor.fetch_command().get() is None
#
#     command_actor.stop()
#
#
# @patch('src.main.actors.commander_actor.Consumer')
# @patch('src.main.actors.commander_actor.StateMachineActor.start')
# def test_consumer_error(mock_state_machine_start, mock_consumer):
#     mock_state_machine_proxy = MagicMock()
#     mock_state_machine_start.return_value.proxy.return_value = mock_state_machine_proxy
#
#     command_actor = CommanderActor.start(mock_state_machine_proxy, COMMAND_TOPIC, BROKER).proxy()
#
#     error_msg = MagicMock()
#     error_msg.error.return_value = "error"
#     mock_consumer.return_value.poll.return_value = error_msg
#
#     with patch("builtins.print") as mock_print:
#         command_actor.fetch_command().get()
#
#         mock_print.assert_called_once_with("Consumer error: error")
#
#     command_actor.stop()
#
#
# @patch('src.main.actors.commander_actor.Consumer')
# @patch('src.main.actors.commander_actor.StateMachineActor.start')
# def test_no_message(mock_state_machine_start, mock_consumer):
#     mock_state_machine_proxy = MagicMock()
#     mock_state_machine_start.return_value.proxy.return_value = mock_state_machine_proxy
#
#     command_actor = CommanderActor.start(mock_state_machine_proxy, COMMAND_TOPIC, BROKER).proxy()
#
#     mock_consumer.return_value.poll.return_value = None
#
#     assert command_actor.fetch_command().get() is None
#
#     command_actor.stop()

