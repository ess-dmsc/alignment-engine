import time
import unittest
from unittest.mock import MagicMock

import pykka
import pytest
from pykka import ActorDeadError
from streaming_data_types import deserialise_f144, serialise_x5f2, deserialise_x5f2

from tests.doubles.producer import ProducerSpy
from alignment_engine.main.actors.producer_actor import ProducerActor, ProducerLogic


class TestProducerActor:
    @pytest.fixture
    def setup(self):
        self.producer_supervisor_mock = MagicMock()
        self.interpolator_actor_mock = MagicMock()
        self.fitter_actor_mock = MagicMock()
        self.producer_logic_mock = MagicMock(spec=ProducerLogic)
        self.actor_ref = ProducerActor.start(self.producer_supervisor_mock, self.producer_logic_mock)
        self.actor_proxy = self.actor_ref.proxy()
        yield
        pykka.ActorRegistry.stop_all()

    def test_on_data_received_tells_self(self, setup):
        some_data = {'command': 'START'}

        with unittest.mock.patch.object(self.actor_ref, 'tell') as mock_tell:
            self.actor_proxy.on_data_received(some_data).get()
            mock_tell.assert_called_once_with(some_data)

    def test_stop_stops_producer_logic_and_super(self, setup):
        self.actor_proxy.stop().get()
        assert self.producer_logic_mock.stop.called

    def test_on_receive_ignores_incorrect_commands(self, setup):
        self.actor_proxy.on_receive({'command': 'INCORRECT_COMMAND'}).get()
        self.producer_logic_mock.produce_message.assert_not_called()
        self.producer_logic_mock.stop.assert_not_called()

    def test_stop_during_message_production(self, setup):
        def produce_messages_long(message):
            time.sleep(0.2)  # sleep to simulate long running task

        self.producer_logic_mock.produce_message.side_effect = produce_messages_long

        self.actor_ref.tell({'command': 'START'})
        time.sleep(0.1)

        self.actor_proxy.on_receive({'command': 'STOP'}).get()
        assert self.producer_logic_mock.stop.called


def test_produce_fit_params():
    message = {
        'data': {
            'sender1': {
                'fit_function': 'gauss',
                'fit_params': [0, 100, 5, 1],
                'r_squared': 0.99,
            }
        }
    }

    producer_spy = ProducerSpy({})
    topic = 'test_topic'

    producer_logic = ProducerLogic(producer_spy, topic)
    producer_logic.produce_message(message)

    received_data = deserialise_f144(producer_spy.data[-1]['value'])

    assert received_data.source_name == 'sender1'
    assert received_data.value.tolist() == [0, 100, 5, 1]


def test_produce_plot_data():
    message = {
        'data': {
            'sender1': [0, 1, 2, 3, 4],
            'sender2': [0, 2, 4, 6, 8],
        }
    }

    producer_spy = ProducerSpy({})
    topic = 'test_topic'

    producer_logic = ProducerLogic(producer_spy, topic)
    producer_logic.produce_message(message)

    received_data_1 = deserialise_f144(producer_spy.data[0]['value'])
    received_data_2 = deserialise_f144(producer_spy.data[1]['value'])

    assert received_data_1.source_name == 'sender1'
    assert received_data_1.value.tolist() == [0, 1, 2, 3, 4]
    assert received_data_2.source_name == 'sender2'
    assert received_data_2.value.tolist() == [0, 2, 4, 6, 8]
    assert received_data_1.timestamp_unix_ns == received_data_2.timestamp_unix_ns


def test_produce_status_message():
    message = [
        "TestSoftware",
        "1.0.0",
        "TestService",
        "TestHost",
        12345,
        1000,
        '{"status": "active"}',
    ]

    # Serialize the message with your existing function
    serialized_message = serialise_x5f2(*message)

    # Construct the data to be sent to Kafka
    data = {'status': message}

    producer_spy = ProducerSpy({})
    topic = 'test_topic'

    producer_logic = ProducerLogic(producer_spy, topic)
    producer_logic.produce_message(data)

    received_message = deserialise_x5f2(producer_spy.data[-1]['value'])

    assert received_message == deserialise_x5f2(serialized_message)


def test_produce_none_message():
    message = None

    producer_spy = ProducerSpy({})
    topic = 'test_topic'

    producer_logic = ProducerLogic(producer_spy, topic)
    producer_logic.produce_message(message)

    assert len(producer_spy.data) == 0


def test_produce_empty_message():
    message = {}

    producer_spy = ProducerSpy({})
    topic = 'test_topic'

    producer_logic = ProducerLogic(producer_spy, topic)
    producer_logic.produce_message(message)

    assert len(producer_spy.data) == 0


def test_produce_message_with_no_data_or_status():
    message = {
        'no_data_or_status': {
            'sender1': {
                'fit_function': 'gauss',
                'fit_params': [0, 100, 5, 1],
                'r_squared': 0.99,
            }
        }
    }

    producer_spy = ProducerSpy({})
    topic = 'test_topic'

    producer_logic = ProducerLogic(producer_spy, topic)
    producer_logic.produce_message(message)

    assert len(producer_spy.data) == 0


def test_process_message_throws_exception():
    message = {
        'data': {
            'sender1': 'This data should not pass.'
        }
    }

    producer_spy = ProducerSpy({})
    topic = 'test_topic'

    producer_logic = ProducerLogic(producer_spy, topic)
    producer_logic.produce_message(message)

    assert len(producer_spy.data) == 0


def test_producer_throws_exception():
    message = {
        'data': {
            'sender1': [0, 1, 2, 3, 4]
        }
    }

    producer_spy = ProducerSpy({}, should_throw=True)
    topic = 'test_topic'

    producer_logic = ProducerLogic(producer_spy, topic)
    producer_logic.produce_message(message)

    assert len(producer_spy.data) == 0

