import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor, as_completed
from unittest.mock import MagicMock

import numpy as np
import pykka
import pytest
from pykka import ActorDeadError

from src.main.actors.consumer_actor import ConsumerActor, ConsumerLogic
from tests.doubles.consumer import ConsumerStub

from streaming_data_types import serialise_f144, serialise_ev44, deserialise_f144, deserialise_ev44

np.random.seed(0)


class TestConsumerActor:
    @pytest.fixture
    def setup(self):
        self.data_handler_actor_mock = MagicMock()
        self.consumer_supervisor_mock = MagicMock()
        self.consumer_logic_mock = MagicMock(spec=ConsumerLogic)
        self.actor_ref = ConsumerActor.start(self.consumer_supervisor_mock, self.data_handler_actor_mock, self.consumer_logic_mock)
        self.actor_proxy = self.actor_ref.proxy()
        yield
        pykka.ActorRegistry.stop_all()

    def test_on_start_consumes_messages(self, setup):
        self.actor_ref.tell({'command': 'START'})
        time.sleep(0.01)
        self.consumer_logic_mock.consume_message.assert_called_once()

    def test_on_receive_forwards_message_to_data_handler_actor(self, setup):
        some_data = [1, 2, 3]
        self.actor_proxy.on_receive({'data': some_data}).get()
        self.data_handler_actor_mock.tell.assert_called_once_with({'data': some_data})

    def test_on_receive_starts_and_stops_consuming_messages(self, setup):
        self.actor_ref.tell({'command': 'START'})
        time.sleep(0.01)
        self.consumer_logic_mock.consume_message.assert_called_once()
        self.actor_proxy.on_receive({'command': 'STOP'}).get()
        assert self.consumer_logic_mock.stop.called

    def test_on_data_received_tells_self(self, setup):
        some_data = [1, 2, 3]

        with unittest.mock.patch.object(self.actor_ref, 'tell') as mock_tell:
            self.actor_proxy.on_data_received(some_data).get()
            mock_tell.assert_called_once_with(some_data)

    def test_stop_stops_consumer_logic_and_super(self, setup):
        self.actor_proxy.stop().get()
        assert self.consumer_logic_mock.stop.called

    def test_consumer_actor_concurrency(self, setup):
        thread_count = 10
        message_count = 1000
        some_data = [1, 2, 3]

        self.actor_ref.tell({'command': 'START'})
        time.sleep(0.01)

        def send_messages():
            for _ in range(message_count):
                self.actor_proxy.on_receive({'data': some_data}).get()

        threads = [threading.Thread(target=send_messages) for _ in range(thread_count)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert self.data_handler_actor_mock.tell.call_count == thread_count * message_count

    def test_on_receive_ignores_incorrect_commands(self, setup):
        self.actor_proxy.on_receive({'command': 'INCORRECT_COMMAND'}).get()
        self.consumer_logic_mock.stop.assert_not_called()
        self.data_handler_actor_mock.on_receive.assert_not_called()

    def test_high_load(self, setup):
        message_count = 10000  # large number of messages
        some_data = np.random.rand(1000, 1000)  # large data

        self.actor_ref.tell({'command': 'START'})
        time.sleep(0.01)

        def send_messages():
            for _ in range(message_count):
                self.actor_proxy.on_receive({'data': some_data}).get()

        thread = threading.Thread(target=send_messages)
        thread.start()
        thread.join()

        assert self.data_handler_actor_mock.tell.call_count == message_count

    def test_stop_during_message_consumption(self, setup):
        def consume_messages_long():
            time.sleep(0.2)  # sleep to simulate long running task

        self.consumer_logic_mock.consume_message.side_effect = consume_messages_long

        self.actor_ref.tell({'command': 'START'})
        time.sleep(0.1)

        self.actor_proxy.on_receive({'command': 'STOP'}).get()
        assert self.consumer_logic_mock.stop.called
        with pytest.raises(ActorDeadError):
            self.actor_proxy.on_receive({'data': [1, 2, 3]}).get()


class TestConsumerActorConcurrency:
    @pytest.fixture
    def setup(self):
        self.data_handler_actor_mocks = [MagicMock() for _ in range(10)]
        self.consumer_supervisor_mocks = [MagicMock() for _ in range(10)]
        self.consumer_logic_mocks = [MagicMock(spec=ConsumerLogic) for _ in range(10)]
        self.actor_refs = [ConsumerActor.start(self.consumer_supervisor_mocks[i],
                                               self.data_handler_actor_mocks[i],
                                               self.consumer_logic_mocks[i]) for i in range(10)]
        self.actor_proxies = [ref.proxy() for ref in self.actor_refs]
        yield
        pykka.ActorRegistry.stop_all()

    def test_multiple_actors_concurrent_execution(self, setup):
        message_count = 100
        some_data = [1, 2, 3]
        messages = [{'data': some_data} for _ in range(message_count)]

        for actor in self.actor_refs:
            actor.tell({'command': 'START'})
        time.sleep(0.01)

        def send_messages(actor_proxy, messages):
            for message in messages:
                actor_proxy.on_receive(message).get()

        threads = [threading.Thread(target=send_messages, args=(actor_proxy, messages))
                   for actor_proxy in self.actor_proxies]

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        for data_handler_actor_mock in self.data_handler_actor_mocks:
            assert data_handler_actor_mock.tell.call_count == message_count


class TestConsumerActorStatus:
    @pytest.fixture
    def setup(self):
        self.data_handler_actor_mocks = [MagicMock() for _ in range(10)]
        self.consumer_supervisor_mocks = [MagicMock() for _ in range(10)]
        self.consumer_logic_mocks = [MagicMock(spec=ConsumerLogic) for _ in range(10)]
        self.actor_refs = [ConsumerActor.start(self.consumer_supervisor_mocks[i],
                                               self.data_handler_actor_mocks[i],
                                               self.consumer_logic_mocks[i]) for i in range(10)]
        self.actor_proxies = [ref.proxy() for ref in self.actor_refs]
        yield
        pykka.ActorRegistry.stop_all()

    def test_consumer_supervisor_gets_status_concurrently(self, setup):
        def get_status(actor_proxy):
            return actor_proxy.get_status().get()

        for actor in self.actor_refs:
            actor.tell({'command': 'START'})
        time.sleep(0.01)

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_results = {executor.submit(get_status, actor_proxy): actor_proxy for actor_proxy in self.actor_proxies}

        for future in as_completed(future_results):
            actor_proxy = future_results[future]
            try:
                data = future.result()
            except Exception as exc:
                print(f'{actor_proxy} generated an exception: {exc}')
            else:
                assert data == 'RUNNING'

    def test_consumer_supervisor_gets_status_concurrently_if_stopped_first(self, setup):
        def get_status(actor_proxy):
            return actor_proxy.get_status().get()

        for actor_proxy in self.actor_proxies:
            actor_proxy.on_receive({'command': 'STOP'}).get()

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_results = {executor.submit(get_status, actor_proxy): actor_proxy for actor_proxy in self.actor_proxies}

        for future in as_completed(future_results):
            with pytest.raises(ActorDeadError):
                data = future.result()

    def test_consumer_supervisor_gets_status_concurrently_if_stopped_after(self, setup):
        def get_status(actor_proxy):
            return actor_proxy.get_status().get()

        for actor in self.actor_refs:
            actor.tell({'command': 'START'})
        time.sleep(0.01)

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_results = {executor.submit(get_status, actor_proxy): actor_proxy for actor_proxy in self.actor_proxies}

        for actor_proxy in self.actor_proxies:
            actor_proxy.on_receive({'command': 'STOP'}).get()

        for future in as_completed(future_results):
            actor_proxy = future_results[future]
            try:
                data = future.result()
            except Exception as exc:
                print(f'{actor_proxy} generated an exception: {exc}')
            else:
                assert data == 'RUNNING'


def generate_fake_ev44_events(num_messages, source_name="ev44_source_1"):
    events = []
    for i in range(num_messages):
        tofs = np.arange(0, np.random.randint(10, 50), 1)
        det_ids = [1] * len(tofs)
        time_ns = time.time_ns()
        events.append(
            serialise_ev44(source_name, time_ns, [time_ns], [0], tofs, det_ids)
        )
    return events


def generate_fake_f144_data(num_messages, source_name="f144_source_1"):
    data = []
    for i in range(num_messages):
        data.append(
            serialise_f144(source_name, np.random.uniform(), time.time_ns())
        )
    return data


def test_consume_f144_data():
    num_messages = 10
    f144_data = generate_fake_f144_data(num_messages)

    consumer_stub = ConsumerStub({})
    for i, data in enumerate(f144_data):
        consumer_stub.add_message('topic', 0, i+1, None, data)

    received_data = []

    def mock_callback(fake_data):
        command = fake_data.get('command', None)
        if command:
            return
        received_data.append(fake_data)

    consumer_logic = ConsumerLogic(consumer_stub, mock_callback)

    for i in range(num_messages):
        consumer_logic.consume_message()

    assert received_data == [{'data': deserialise_f144(data)} for data in f144_data]


def test_consume_ev44_data():
    num_messages = 10
    ev44_data = generate_fake_ev44_events(num_messages)
    expected_data = [{'data': deserialise_ev44(data)} for data in ev44_data]

    consumer_stub = ConsumerStub({})
    for i, data in enumerate(ev44_data):
        consumer_stub.add_message('topic', 0, i+1, None, data)

    received_data = []

    def mock_callback(fake_data):
        command = fake_data.get('command', None)
        if command:
            return
        received_data.append(fake_data)

    consumer_logic = ConsumerLogic(consumer_stub, mock_callback)

    for i in range(num_messages):
        consumer_logic.consume_message()

    for received, expected in zip(received_data, expected_data):
        assert received['data'].source_name == expected['data'].source_name
        assert received['data'].message_id == expected['data'].message_id
        assert np.array_equal(received['data'].reference_time, expected['data'].reference_time)
        assert np.array_equal(received['data'].reference_time_index, expected['data'].reference_time_index)
        assert np.array_equal(received['data'].time_of_flight, expected['data'].time_of_flight)
        assert np.array_equal(received['data'].pixel_id, expected['data'].pixel_id)


def test_consume_f144_data_with_different_source_names():
    f144_data_1 = generate_fake_f144_data(10, source_name="f144_source_1")
    f144_data_2 = generate_fake_f144_data(10, source_name="f144_source_2")
    f144_data = f144_data_1 + f144_data_2

    consumer_stub = ConsumerStub({})
    for i, data in enumerate(f144_data):
        consumer_stub.add_message('topic', 0, i+1, None, data)

    received_data = []

    def mock_callback(fake_data):
        command = fake_data.get('command', None)
        if command:
            return
        received_data.append(fake_data)

    consumer_logic = ConsumerLogic(consumer_stub, mock_callback, source_name="f144_source_1")

    for i in range(len(f144_data)):
        consumer_logic.consume_message()

    assert received_data == [{'data': deserialise_f144(data)} for data in f144_data_1]


def test_consume_ev44_data_with_different_source_names():
    ev44_data_1 = generate_fake_ev44_events(10, source_name="ev44_source_1")
    ev44_data_2 = generate_fake_ev44_events(10, source_name="ev44_source_2")
    ev44_data = ev44_data_1 + ev44_data_2

    consumer_stub = ConsumerStub({})
    for i, data in enumerate(ev44_data):
        consumer_stub.add_message('topic', 0, i+1, None, data)

    received_data = []

    def mock_callback(fake_data):
        command = fake_data.get('command', None)
        if command:
            return
        received_data.append(fake_data)

    consumer_logic = ConsumerLogic(consumer_stub, mock_callback, source_name="ev44_source_1")

    for i in range(len(ev44_data)):
        consumer_logic.consume_message()

    expected_data = [{'data': deserialise_ev44(data)} for data in ev44_data_1]

    for received, expected in zip(received_data, expected_data):
        assert received['data'].source_name == expected['data'].source_name
        assert received['data'].message_id == expected['data'].message_id
        assert np.array_equal(received['data'].reference_time, expected['data'].reference_time)
        assert np.array_equal(received['data'].reference_time_index, expected['data'].reference_time_index)
        assert np.array_equal(received['data'].time_of_flight, expected['data'].time_of_flight)
        assert np.array_equal(received['data'].pixel_id, expected['data'].pixel_id)


def test_consume_data_concurrently_with_two_consumers_with_different_source_names():
    f144_data_1 = generate_fake_f144_data(10, source_name="f144_source_1")
    f144_data_2 = generate_fake_f144_data(10, source_name="f144_source_2")
    f144_data = f144_data_1 + f144_data_2

    consumer_stub_1 = ConsumerStub({})
    consumer_stub_2 = ConsumerStub({})
    for i, data in enumerate(f144_data):
        consumer_stub_1.add_message('topic', 0, i+1, None, data)
        consumer_stub_2.add_message('topic', 0, i + 1, None, data)

    received_data_1 = []
    received_data_2 = []

    def mock_callback_1(fake_data):
        command = fake_data.get('command', None)
        if command:
            return
        received_data_1.append(fake_data)

    def mock_callback_2(fake_data):
        command = fake_data.get('command', None)
        if command:
            return
        received_data_2.append(fake_data)

    consumer_logic_1 = ConsumerLogic(consumer_stub_1, mock_callback_1, source_name="f144_source_1")
    consumer_logic_2 = ConsumerLogic(consumer_stub_2, mock_callback_2, source_name="f144_source_2")

    for i in range(len(f144_data)):
        consumer_logic_1.consume_message()
        consumer_logic_2.consume_message()

    assert received_data_1 == [{'data': deserialise_f144(data)} for data in f144_data_1]
    assert received_data_2 == [{'data': deserialise_f144(data)} for data in f144_data_2]


def test_consume_faulty_message():
    faulty_data = [b'faulty message']

    consumer_stub = ConsumerStub({})
    for i, data in enumerate(faulty_data):
        consumer_stub.add_message('topic', 0, i+1, None, data)

    received_data = []

    def mock_callback(fake_data):
        command = fake_data.get('command', None)
        if command:
            return
        received_data.append(fake_data)

    consumer_logic = ConsumerLogic(consumer_stub, mock_callback)

    for i in range(len(faulty_data)):
        consumer_logic.consume_message()

    assert received_data == []


def test_consume_with_kafka_exception():
    good_data = generate_fake_f144_data(1)
    faulty_data = ['faulty message']

    all_data = good_data + faulty_data

    consumer_stub = ConsumerStub({})
    for i, data in enumerate(all_data):
        consumer_stub.add_message('topic', 0, i+1, None, data)

    received_data = []

    def mock_callback(fake_data):
        command = fake_data.get('command', None)
        if command:
            return
        received_data.append(fake_data)

    consumer_logic = ConsumerLogic(consumer_stub, mock_callback)

    for i in range(len(all_data)):
        consumer_logic.consume_message()

    assert received_data == [{'data': deserialise_f144(data)} for data in good_data]


def test_consume_with_unicode_decode_error():
    good_data = generate_fake_f144_data(1)
    faulty_data = [b'\x80abc']

    all_data = good_data + faulty_data

    consumer_stub = ConsumerStub({})
    for i, data in enumerate(all_data):
        consumer_stub.add_message('topic', 0, i+1, None, data)

    received_data = []

    def mock_callback(fake_data):
        command = fake_data.get('command', None)
        if command:
            return
        received_data.append(fake_data)

    consumer_logic = ConsumerLogic(consumer_stub, mock_callback)

    for i in range(len(all_data)):
        consumer_logic.consume_message()

    assert received_data == [{'data': deserialise_f144(data)} for data in good_data]
