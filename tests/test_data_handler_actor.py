import threading
import time
from unittest.mock import MagicMock

import pykka
import pytest
import numpy as np
from pykka import ActorDeadError
from queue import Queue
from streaming_data_types import deserialise_f144, deserialise_ev44, serialise_f144, serialise_ev44

from src.main.actors.data_handler_actor import DataHandlerActor, DataHandlerLogic
from src.main.actors.interpolator_actor import InterpolatorActor


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


class TestDataHandlerActor:
    @pytest.fixture
    def data_handler_setup(self):
        self.data_handler_supervisor_mock = MagicMock()
        self.data_handler_logic_mock = MagicMock(spec=DataHandlerLogic)
        self.interpolator_actor_mock = MagicMock(spec=InterpolatorActor)
        self.actor_ref = DataHandlerActor.start(
            self.data_handler_supervisor_mock,
            self.interpolator_actor_mock,
            self.data_handler_logic_mock)
        self.actor_proxy = self.actor_ref.proxy()
        yield
        pykka.ActorRegistry.stop_all()

    def test_on_start_data_handler(self, data_handler_setup):
        self.actor_proxy.on_receive({'command': 'START'}).get()
        assert self.actor_proxy.get_status().get() == 'RUNNING'

    def test_on_receive_data_data_handler(self, data_handler_setup):
        fake_data = generate_fake_ev44_events(1)[0]
        self.actor_proxy.on_receive({'data': fake_data}).get()
        self.data_handler_logic_mock.on_data_received.assert_called_once()

    def test_on_receive_stop_data_handler(self, data_handler_setup):
        self.actor_proxy.on_receive({'command': 'STOP'}).get()
        assert self.data_handler_logic_mock.stop.called

    def test_data_handler_actor_concurrency(self, data_handler_setup):
        thread_count = 10
        message_count = 1000
        fake_data = generate_fake_ev44_events(1)[0]

        def send_messages():
            for _ in range(message_count):
                self.actor_proxy.on_receive({'data': fake_data}).get()

        threads = [threading.Thread(target=send_messages) for _ in range(thread_count)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert self.data_handler_logic_mock.on_data_received.call_count == thread_count * message_count

    def test_on_receive_unknown_command_data_handler(self, data_handler_setup):
        self.actor_proxy.on_receive('UNKNOWN_COMMAND').get()
        self.data_handler_logic_mock.on_data_received.assert_not_called()

    def test_on_receive_bad_data_data_handler(self, data_handler_setup):
        self.actor_proxy.on_receive({'bad_key': 'bad_data'}).get()
        self.data_handler_logic_mock.on_data_received.assert_not_called()

    def test_stop_during_data_processing(self, data_handler_setup):
        fake_data = generate_fake_ev44_events(1)[0]

        def on_data_received_long(data):
            time.sleep(0.2)  # sleep to simulate long running task

        self.data_handler_logic_mock.on_data_received.side_effect = on_data_received_long

        thread = threading.Thread(target=lambda: self.actor_proxy.on_receive({'data': fake_data}).get())
        thread.start()

        time.sleep(0.1)  # sleep to ensure on_data_received_long() has started
        self.actor_proxy.on_receive({'command': 'STOP'}).get()

        assert self.data_handler_logic_mock.stop.called
        with pytest.raises(ActorDeadError):
            self.actor_proxy.on_receive({'data': fake_data}).get()

        thread.join()

    def test_exception_on_data_received_does_not_stop_actor(self, data_handler_setup):
        self.data_handler_logic_mock.on_data_received.side_effect = Exception("Test exception")
        fake_data = generate_fake_ev44_events(1)[0]
        self.actor_proxy.on_receive({'data': fake_data}).get()

        assert self.actor_proxy.get_status().get() == 'RUNNING'
        assert self.data_handler_logic_mock.on_data_received.call_count == 1

    def test_exception_on_data_received_with_multiple_threads(self, data_handler_setup):
        self.data_handler_logic_mock.on_data_received.side_effect = Exception("Test exception")
        thread_count = 10
        fake_data = generate_fake_ev44_events(1)[0]

        def send_messages():
            for _ in range(10):
                self.actor_proxy.on_receive({'data': fake_data}).get()

        threads = [threading.Thread(target=send_messages) for _ in range(thread_count)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert self.actor_proxy.get_status().get() == 'RUNNING'
        assert self.data_handler_logic_mock.on_data_received.call_count == thread_count * 10


class TestDataHandlerActorConcurrency:
    @pytest.fixture
    def data_handler_setup(self):
        self.data_handler_supervisor_mocks = [MagicMock() for _ in range(10)]
        self.data_handler_logic_mocks = [MagicMock(spec=DataHandlerLogic) for _ in range(10)]
        self.interpolator_actor_mock = MagicMock(spec=InterpolatorActor)
        self.actor_refs = [DataHandlerActor.start(self.data_handler_supervisor_mocks[i],
                                                  self.interpolator_actor_mock,
                                                  self.data_handler_logic_mocks[i]) for i in range(10)]
        self.actor_proxies = [ref.proxy() for ref in self.actor_refs]
        yield
        pykka.ActorRegistry.stop_all()

    def test_multiple_actors_start_concurrently(self, data_handler_setup):
        for actor_proxy in self.actor_proxies:
            actor_proxy.on_receive({'command': 'START'}).get()

        for data_handler_logic_mock in self.data_handler_logic_mocks:
            assert data_handler_logic_mock.start.called

    def test_multiple_actors_stop_concurrently(self, data_handler_setup):
        for actor_proxy in self.actor_proxies:
            actor_proxy.on_receive({'command': 'STOP'}).get()

        for data_handler_logic_mock in self.data_handler_logic_mocks:
            assert data_handler_logic_mock.stop.called

    def test_multiple_actors_reset_concurrently(self, data_handler_setup):
        for actor_proxy in self.actor_proxies:
            actor_proxy.on_receive({'command': 'RESET'}).get()

        for data_handler_logic_mock in self.data_handler_logic_mocks:
            assert data_handler_logic_mock.reset.called

    def test_multiple_actors_on_receive_concurrently(self, data_handler_setup):
        fake_data = generate_fake_ev44_events(1)[0]

        def send_messages(actor_proxy):
            for _ in range(10):
                actor_proxy.on_receive({'data': fake_data}).get()

        threads = [threading.Thread(target=send_messages, args=(actor_proxy,)) for actor_proxy in self.actor_proxies]

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        for data_handler_logic_mock in self.data_handler_logic_mocks:
            assert data_handler_logic_mock.on_data_received.call_count == 10

class TestDataHandlerActorSendToInterpolator:

    @pytest.fixture
    def data_handler_setup(self):
        self.data_handler_supervisor_mock = MagicMock()
        self.data_handler_logic_mock = MagicMock(spec=DataHandlerLogic)
        self.data_handler_logic_mock.configure_mock(value_data=[], time_data=[])
        self.interpolator_actor_mock = MagicMock(spec=InterpolatorActor)
        self.interpolator_actor_mock.tell = MagicMock()
        self.actor_ref = DataHandlerActor.start(
            self.data_handler_supervisor_mock,
            self.interpolator_actor_mock,
            self.data_handler_logic_mock
        )
        self.actor_proxy = self.actor_ref.proxy()
        yield
        pykka.ActorRegistry.stop_all()

    def test_on_receive_data_calls_send_to_interpolator(self, data_handler_setup):
        fake_data = generate_fake_ev44_events(1)[0]
        self.actor_proxy.on_receive({'data': fake_data}).get()

        expected_data = {
            'sender': self.actor_ref.actor_urn,
            'data': {
                'value': self.data_handler_logic_mock.value_data,
                'time': self.data_handler_logic_mock.time_data,
            }
        }

        self.interpolator_actor_mock.tell.assert_called_once_with(expected_data)


class TestDataHandlerActorSendToInterpolatorConcurrency:
    @pytest.fixture
    def data_handler_setup(self):
        self.data_handler_supervisor_mocks = [MagicMock() for _ in range(10)]
        self.data_handler_logic_mocks = [MagicMock(spec=DataHandlerLogic) for _ in range(10)]
        for mock in self.data_handler_logic_mocks:
            mock.configure_mock(value_data=[], time_data=[])
        self.interpolator_actor_mock = MagicMock(spec=InterpolatorActor)
        self.interpolator_actor_mock.tell = MagicMock()
        self.actor_refs = [DataHandlerActor.start(self.data_handler_supervisor_mocks[i],
                                                  self.interpolator_actor_mock,
                                                  self.data_handler_logic_mocks[i]) for i in range(10)]
        self.actor_proxies = [ref.proxy() for ref in self.actor_refs]
        self.calls_queue = Queue()
        yield
        pykka.ActorRegistry.stop_all()

    def test_multiple_actors_send_data_concurrently(self, data_handler_setup):
        fake_data = generate_fake_ev44_events(1)[0]

        def send_messages(actor_proxy, data_handler_logic_mock, actor_ref):
            for _ in range(10):
                actor_proxy.on_receive({'data': fake_data}).get()

                expected_data = {
                    'sender': actor_ref.actor_urn,
                    'data': {
                        'value': data_handler_logic_mock.value_data,
                        'time': data_handler_logic_mock.time_data,
                    }
                }

                self.interpolator_actor_mock.tell(expected_data)
                self.calls_queue.put(expected_data)

        threads = [
            threading.Thread(target=send_messages, args=(self.actor_proxies[i], self.data_handler_logic_mocks[i], self.actor_refs[i]))
            for i in range(10)
        ]

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert self.calls_queue.qsize() == 10 * 10



@pytest.fixture
def data_handler():
    return DataHandlerLogic()


def test_on_data_received_f144(data_handler):
    fake_data = generate_fake_f144_data(10)
    deserialized_data = [deserialise_f144(data) for data in fake_data]

    for data in deserialized_data:
        data_handler.on_data_received({'data': data})

    assert len(data_handler.value_data) == 10
    assert len(data_handler.time_data) == 10
    for i, data in enumerate(deserialized_data):
        assert data_handler.value_data[i] == data.value
        assert data_handler.time_data[i] == data.timestamp_unix_ns

    data_handler.reset()


def test_on_data_received_ev44(data_handler):
    fake_data = generate_fake_ev44_events(10)
    deserialized_data = [deserialise_ev44(data) for data in fake_data]

    for data in deserialized_data:
        data_handler.on_data_received({'data': data})

    assert len(data_handler.value_data) == 10
    assert len(data_handler.time_data) == 10
    for i, data in enumerate(deserialized_data):
        assert data_handler.value_data[i] == len(data.time_of_flight)
        assert data_handler.time_data[i] == data.reference_time[0] + np.min(data.time_of_flight)

    data_handler.reset()


def test_get_data_type(data_handler):
    fake_ev44_data = generate_fake_ev44_events(10)
    deserialized_ev44_data = deserialise_ev44(fake_ev44_data[0])
    assert data_handler.get_data_type(deserialized_ev44_data) == 'ev44'

    fake_f144_data = generate_fake_f144_data(10)
    deserialized_f144_data = deserialise_f144(fake_f144_data[0])
    assert data_handler.get_data_type(deserialized_f144_data) == 'f144'


def test_stop(data_handler):
    data_handler.stop()
    assert not data_handler.active
    data_handler.reset()


def test_reset(data_handler):
    fake_data = generate_fake_f144_data(10)
    deserialized_data = deserialise_f144(fake_data[0])
    data_handler.on_data_received({'data': deserialized_data})
    data_handler.stop()
    data_handler.reset()
    assert len(data_handler.value_data) == 0
    assert len(data_handler.time_data) == 0
    assert data_handler.active


def test_unexpected_data_type(data_handler):
    class FakeData:
        pass

    fake_data = FakeData()
    assert data_handler.get_data_type(fake_data) is None


def test_none_data(data_handler):
    assert data_handler.get_data_type(None) is None


def test_faulty_ev44_data(data_handler):
    class FaultyEv44:
        def __init__(self):
            self.time_of_flight = None
            self.reference_time = None

    faulty_ev44 = FaultyEv44()
    data_handler.on_data_received({'data': faulty_ev44})
    assert len(data_handler.value_data) == 0
    assert len(data_handler.time_data) == 0


def test_faulty_f144_data(data_handler):
    class FaultyF144:
        def __init__(self):
            self.value = None
            self.timestamp_unix_ns = None

    faulty_f144 = FaultyF144()
    data_handler.on_data_received({'data': faulty_f144})
    assert len(data_handler.value_data) == 0
    assert len(data_handler.time_data) == 0


def test_exception_during_data_processing(data_handler, monkeypatch):
    def mock_crash(*args, **kwargs):
        raise ValueError("Something went wrong!")

    monkeypatch.setattr("src.main.actors.data_handler_actor.DataHandlerLogic.on_data_received", mock_crash)

    with pytest.raises(ValueError):
        fake_data = generate_fake_f144_data(1)
        deserialized_data = deserialise_f144(fake_data[0])
        data_handler.on_data_received(deserialized_data)