import time

import numpy as np
import pytest
from unittest.mock import MagicMock
from pykka import ActorRegistry

from src.main.actors.interpolator_actor import InterpolatorActor, InterpolatorLogic
from tests.doubles.consumer import ConsumerStub
from tests.test_consumer_actor import generate_fake_ev44_events, generate_fake_f144_data
from src.main.actors.consumer_actor import ConsumerActor, ConsumerLogic
from src.main.actors.data_handler_actor import DataHandlerActor, DataHandlerLogic
from src.main.actors.fitter_actor import FitterActor, FitterLogic, gauss_func

from streaming_data_types import deserialise_ev44, deserialise_f144, serialise_ev44, serialise_f144


def generate_simple_fake_ev44_events(num_messages, source_name="ev44_source_1"):
    events = []
    for i in range(num_messages):
        tofs = [0, 1, 2, 3]
        det_ids = [1] * len(tofs)
        time_ns = i
        events.append(
            serialise_ev44(source_name, time_ns, [time_ns], [0], tofs, det_ids)
        )
    return events


def generate_simple_fake_f144_data(num_messages, source_name="f144_source_1"):
    data = []
    for i in range(num_messages):
        data.append(
            serialise_f144(source_name, i, i)
        )
    return data


def generate_gauss_f144_data(num_messages, source_name="f144_source_1"):
    assert num_messages % 2 == 1, "num_messages must be odd"
    vals = np.linspace(0, 10, num_messages).tolist()
    times = np.linspace(0, 1000, num_messages).astype(np.int64).tolist()
    data = []
    for val, t in zip(vals, times):
        data.append(
            serialise_f144(source_name, val, t)
        )
    return data


def generate_gauss_ev44_events(num_messages, source_name="ev44_source_1"):
    assert num_messages % 2 == 1, "num_messages must be odd"
    vals = gauss_func(np.linspace(0, 10, num_messages), 0, 100, 5, 1).astype(np.int64).tolist()
    times = np.linspace(0, 1000, num_messages).astype(np.int64).tolist()
    events = []
    for val, t in zip(vals, times):
        tofs = np.arange(0, val, 1).astype(np.int64).tolist()
        det_ids = [1] * len(tofs)
        events.append(
            serialise_ev44(source_name, t, [t], [0], tofs, det_ids)
        )
    return events


class TestConsumerDataHandlerF144:
    @pytest.fixture
    def setup(self):
        self.f144_data = generate_fake_f144_data(10)
        self.consumer_stub = ConsumerStub({})
        for i, data in enumerate(self.f144_data):
            self.consumer_stub.add_message('topic', 0, i+1, None, data)

        self.data_handler_supervisor_mock = MagicMock()
        self.interpolator_actor_mock = MagicMock(spec=InterpolatorActor)
        self.interpolator_actor_mock.actor_ref.tell = MagicMock()
        self.data_handler_actor_ref = DataHandlerActor.start(
            self.data_handler_supervisor_mock,
            self.interpolator_actor_mock,
            DataHandlerLogic())
        self.data_handler_actor_proxy = self.data_handler_actor_ref.proxy()

        self.consumer_supervisor_mock = MagicMock()
        self.consumer_logic = ConsumerLogic(self.consumer_stub)
        self.consumer_actor_ref = ConsumerActor.start(self.consumer_supervisor_mock, self.data_handler_actor_ref, self.consumer_logic)
        self.consumer_actor_proxy = self.consumer_actor_ref.proxy()
        yield
        ActorRegistry.stop_all()

    def test_consumer_actor(self, setup):
        # Check DataHandlerActor's state
        assert self.data_handler_actor_proxy.get_status().get() == 'RUNNING'

        self.consumer_actor_ref.tell('START')

        assert self.consumer_actor_proxy.get_status().get() == 'RUNNING'

        time.sleep(0.1)

        self.consumer_actor_ref.tell('STOP')

        final_data = self.data_handler_actor_proxy.data_handler_logic.get().value_data
        final_times = self.data_handler_actor_proxy.data_handler_logic.get().time_data

        assert final_data == [deserialise_f144(dat).value for dat in self.f144_data]
        assert final_times == [deserialise_f144(dat).timestamp_unix_ns for dat in self.f144_data]


class TestConsumerDataHandlerEv44:
    @pytest.fixture
    def setup(self):
        self.ev44_data = generate_fake_ev44_events(10)
        self.consumer_stub = ConsumerStub({})
        for i, data in enumerate(self.ev44_data):
            self.consumer_stub.add_message('topic', 0, i+1, None, data)

        self.data_handler_supervisor_mock = MagicMock()
        self.interpolator_actor_mock = MagicMock(spec=InterpolatorActor)
        self.interpolator_actor_mock.actor_ref.tell = MagicMock()
        self.data_handler_actor_ref = DataHandlerActor.start(
            self.data_handler_supervisor_mock,
            self.interpolator_actor_mock,
            DataHandlerLogic())
        self.data_handler_actor_proxy = self.data_handler_actor_ref.proxy()

        self.consumer_supervisor_mock = MagicMock()
        self.consumer_logic = ConsumerLogic(self.consumer_stub)
        self.consumer_actor_ref = ConsumerActor.start(self.consumer_supervisor_mock, self.data_handler_actor_ref, self.consumer_logic)
        self.consumer_actor_proxy = self.consumer_actor_ref.proxy()
        yield
        ActorRegistry.stop_all()

    def test_consumer_actor(self, setup):
        # Check DataHandlerActor's state
        assert self.data_handler_actor_proxy.get_status().get() == 'RUNNING'

        self.consumer_actor_ref.tell('START')

        assert self.consumer_actor_proxy.get_status().get() == 'RUNNING'

        time.sleep(0.1)

        self.consumer_actor_ref.tell('STOP')

        # Check the first message, it should be processed by the ev44 schema
        final_data = self.data_handler_actor_proxy.data_handler_logic.get().value_data
        final_times = self.data_handler_actor_proxy.data_handler_logic.get().time_data

        assert final_data == [len(deserialise_ev44(dat).time_of_flight) for dat in self.ev44_data]
        assert final_times == [deserialise_ev44(dat).time_of_flight[0] + deserialise_ev44(dat).reference_time for dat in self.ev44_data]


class TestConsumerDataHandlerF144AndEv44:
    @pytest.fixture
    def setup(self):
        self.f144_data = generate_fake_f144_data(10)
        self.ev44_data = generate_fake_ev44_events(10)
        self.consumer_stub_f144 = ConsumerStub({})
        self.consumer_stub_ev44 = ConsumerStub({})
        for i, data in enumerate(self.f144_data):
            self.consumer_stub_f144.add_message('topic', 0, i+1, None, data)
        for i, data in enumerate(self.ev44_data):
            self.consumer_stub_ev44.add_message('topic', 0, i+1, None, data)

        self.data_handler_supervisor_mock = MagicMock()
        self.interpolator_actor_mock = MagicMock(spec=InterpolatorActor)
        self.interpolator_actor_mock.actor_ref.tell = MagicMock()
        self.data_handler_actor_refs = [DataHandlerActor.start(
            self.data_handler_supervisor_mock,
            self.interpolator_actor_mock,
            DataHandlerLogic()) for i in range(2)]
        self.data_handler_actor_proxys = [actor.proxy() for actor in self.data_handler_actor_refs]

        self.consumer_supervisor_mock = MagicMock()
        self.consumer_actor_refs = [
            ConsumerActor.start(self.consumer_supervisor_mock, dataHandler, consumer_logic)
            for dataHandler, consumer_logic in zip(self.data_handler_actor_refs, [
                ConsumerLogic(self.consumer_stub_ev44),
                ConsumerLogic(self.consumer_stub_f144),
            ])
        ]
        self.consumer_actor_proxys = [actor.proxy() for actor in self.consumer_actor_refs]
        yield
        ActorRegistry.stop_all()

    def test_consumer_actor(self, setup):
        # Check DataHandlerActor's state
        for data_handler_actor in self.data_handler_actor_proxys:
            assert data_handler_actor.get_status().get() == 'RUNNING'

        for consumer_actor in self.consumer_actor_refs:
            consumer_actor.tell('START')

        for consumer_actor in self.consumer_actor_proxys:
            assert consumer_actor.get_status().get() == 'RUNNING'

        time.sleep(0.1)

        for consumer_actor in self.consumer_actor_refs:
            consumer_actor.tell('STOP')

        final_data_ev44 = self.data_handler_actor_proxys[0].data_handler_logic.get().value_data
        final_times_ev44 = self.data_handler_actor_proxys[0].data_handler_logic.get().time_data
        final_data_f144 = self.data_handler_actor_proxys[1].data_handler_logic.get().value_data
        final_times_f144 = self.data_handler_actor_proxys[1].data_handler_logic.get().time_data

        assert final_data_ev44 == [len(deserialise_ev44(dat).time_of_flight) for dat in self.ev44_data]
        assert final_times_ev44 == [deserialise_ev44(dat).time_of_flight[0] + deserialise_ev44(dat).reference_time for dat in self.ev44_data]
        assert final_data_f144 == [deserialise_f144(dat).value for dat in self.f144_data]
        assert final_times_f144 == [deserialise_f144(dat).timestamp_unix_ns for dat in self.f144_data]


class TestConsumersToInterpolator:
    @pytest.fixture
    def setup(self):
        self.f144_data = generate_simple_fake_f144_data(4)
        self.ev44_data = generate_simple_fake_ev44_events(2)
        self.consumer_stub_f144 = ConsumerStub({})
        self.consumer_stub_ev44 = ConsumerStub({})
        for i, data in enumerate(self.f144_data):
            self.consumer_stub_f144.add_message('topic', 0, i + 1, None, data)
        for i, data in enumerate(self.ev44_data):
            self.consumer_stub_ev44.add_message('topic', 0, i + 1, None, data)

        self.state_machine_actor_mock = MagicMock()
        self.fitting_actor_mock = MagicMock()
        self.interpolator_logic = InterpolatorLogic()
        self.interpolator_actor_ref = InterpolatorActor.start(
            self.state_machine_actor_mock,
            self.fitting_actor_mock,
            self.interpolator_logic)
        self.interpolator_actor_proxy = self.interpolator_actor_ref.proxy()

        self.data_handler_supervisor_mock = MagicMock()
        self.data_handler_actor_refs = [DataHandlerActor.start(
            self.data_handler_supervisor_mock,
            self.interpolator_actor_ref,
            DataHandlerLogic()) for i in range(2)]
        self.data_handler_actor_proxys = [actor.proxy() for actor in self.data_handler_actor_refs]

        self.consumer_supervisor_mock = MagicMock()
        self.consumer_actor_refs = [
            ConsumerActor.start(self.consumer_supervisor_mock, dataHandler, consumer_logic)
            for dataHandler, consumer_logic in zip(self.data_handler_actor_refs, [
                ConsumerLogic(self.consumer_stub_ev44),
                ConsumerLogic(self.consumer_stub_f144),
            ])
        ]
        self.consumer_actor_proxys = [actor.proxy() for actor in self.consumer_actor_refs]
        yield
        ActorRegistry.stop_all()

    def test_two_consumers(self, setup):
        # Check DataHandlerActor's state
        for data_handler_actor in self.data_handler_actor_proxys:
            assert data_handler_actor.get_status().get() == 'RUNNING'

        for consumer_actor in self.consumer_actor_refs:
            consumer_actor.tell('START')

        for consumer_actor in self.consumer_actor_proxys:
            assert consumer_actor.get_status().get() == 'RUNNING'

        time.sleep(0.1)

        for consumer_actor in self.consumer_actor_refs:
            consumer_actor.tell('STOP')

        final_data_ev44 = self.data_handler_actor_proxys[0].data_handler_logic.get().value_data
        final_times_ev44 = self.data_handler_actor_proxys[0].data_handler_logic.get().time_data
        final_data_f144 = self.data_handler_actor_proxys[1].data_handler_logic.get().value_data
        final_times_f144 = self.data_handler_actor_proxys[1].data_handler_logic.get().time_data

        assert final_data_ev44 == [len(deserialise_ev44(dat).time_of_flight) for dat in self.ev44_data]
        assert final_times_ev44 == [deserialise_ev44(dat).time_of_flight[0] + deserialise_ev44(dat).reference_time for dat in self.ev44_data]
        assert final_data_f144 == [deserialise_f144(dat).value for dat in self.f144_data]
        assert final_times_f144 == [deserialise_f144(dat).timestamp_unix_ns for dat in self.f144_data]

        result_dict = self.interpolator_actor_proxy.get_results().get()
        sender_1_result = result_dict[self.data_handler_actor_proxys[0].actor_urn.get()]
        sender_2_result = result_dict[self.data_handler_actor_proxys[1].actor_urn.get()]

        assert sender_1_result.tolist() == [4, 4, 4, 4]
        assert sender_2_result.tolist() == [0, 1, 2, 3]


class TestConsumersToFitter:
    @pytest.fixture
    def setup(self):
        self.f144_data = generate_gauss_f144_data(101)
        self.ev44_data = generate_gauss_ev44_events(201)
        self.consumer_stub_f144 = ConsumerStub({})
        self.consumer_stub_ev44 = ConsumerStub({})
        for i, data in enumerate(self.f144_data):
            self.consumer_stub_f144.add_message('topic', 0, i + 1, None, data)
        for i, data in enumerate(self.ev44_data):
            self.consumer_stub_ev44.add_message('topic', 0, i + 1, None, data)

        self.state_machine_actor_mock = MagicMock()

        self.producer_actor_mock = MagicMock()
        self.fitter_logic = FitterLogic()
        self.fitting_actor_ref = FitterActor.start(
            self.state_machine_actor_mock,
            self.producer_actor_mock,
            self.fitter_logic)
        self.fitting_actor_proxy = self.fitting_actor_ref.proxy()

        self.interpolator_logic = InterpolatorLogic()
        self.interpolator_actor_ref = InterpolatorActor.start(
            self.state_machine_actor_mock,
            self.fitting_actor_ref,
            self.interpolator_logic)
        self.interpolator_actor_proxy = self.interpolator_actor_ref.proxy()

        self.data_handler_supervisor_mock = MagicMock()
        self.data_handler_actor_refs = [DataHandlerActor.start(
            self.data_handler_supervisor_mock,
            self.interpolator_actor_ref,
            DataHandlerLogic()) for i in range(2)]
        self.data_handler_actor_proxys = [actor.proxy() for actor in self.data_handler_actor_refs]

        self.consumer_supervisor_mock = MagicMock()
        self.consumer_actor_refs = [
            ConsumerActor.start(self.consumer_supervisor_mock, dataHandler, consumer_logic)
            for dataHandler, consumer_logic in zip(self.data_handler_actor_refs, [
                ConsumerLogic(self.consumer_stub_ev44),
                ConsumerLogic(self.consumer_stub_f144),
            ])
        ]
        self.consumer_actor_proxys = [actor.proxy() for actor in self.consumer_actor_refs]
        yield
        ActorRegistry.stop_all()

    def test_two_consumers(self, setup):
        # Check DataHandlerActor's state
        for data_handler_actor in self.data_handler_actor_proxys:
            assert data_handler_actor.get_status().get() == 'RUNNING'

        conf = {
            'control_signals': [self.data_handler_actor_proxys[1].actor_urn.get()],
            'readout_signals': [self.data_handler_actor_proxys[0].actor_urn.get()],
            'fit_function': 'gauss'
        }
        self.fitting_actor_ref.tell({'CONF': conf})

        for consumer_actor in self.consumer_actor_refs:
            consumer_actor.tell('START')

        for consumer_actor in self.consumer_actor_proxys:
            assert consumer_actor.get_status().get() == 'RUNNING'

        time.sleep(0.1)

        for consumer_actor in self.consumer_actor_refs:
            consumer_actor.tell('STOP')

        final_data_ev44 = self.data_handler_actor_proxys[0].data_handler_logic.get().value_data
        final_times_ev44 = self.data_handler_actor_proxys[0].data_handler_logic.get().time_data
        final_data_f144 = self.data_handler_actor_proxys[1].data_handler_logic.get().value_data
        final_times_f144 = self.data_handler_actor_proxys[1].data_handler_logic.get().time_data

        # Just because there sometimes is not any data for some events.
        deserialised_data = [deserialise_ev44(dat) for dat in self.ev44_data]
        expected_times = [
            (dat.time_of_flight[0] + dat.reference_time if len(dat.time_of_flight) > 0 else dat.reference_time) for dat
            in deserialised_data
        ]

        assert final_data_ev44 == [len(deserialise_ev44(dat).time_of_flight) for dat in self.ev44_data]
        assert final_times_ev44 == expected_times
        assert final_data_f144 == [deserialise_f144(dat).value for dat in self.f144_data]
        assert final_times_f144 == [deserialise_f144(dat).timestamp_unix_ns for dat in self.f144_data]

        result = self.fitting_actor_proxy.get_results().get()

        assert np.allclose(result[self.data_handler_actor_proxys[1].actor_urn.get()]['fit_params'][2], 5, atol=0.1)

