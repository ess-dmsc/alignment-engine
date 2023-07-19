import threading
import time
import pytest
from unittest.mock import MagicMock
from pykka import ActorRegistry

from src.main.actors.interpolator_actor import InterpolatorActor
from tests.doubles.consumer import ConsumerStub
from tests.test_consumer_actor import generate_fake_ev44_events, generate_fake_f144_data
from src.main.actors.consumer_actor import ConsumerActor, ConsumerLogic
from src.main.actors.data_handler_actor import DataHandlerActor, DataHandlerLogic

from streaming_data_types import deserialise_ev44, deserialise_f144

class TestConsumerActor:
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

        time.sleep(0.1)

        self.consumer_actor_ref.tell('STOP')

        # Check the first message, it should be processed by the ev44 schema
        final_data = self.data_handler_actor_proxy.data_handler_logic.get().value_data

        assert final_data == [deserialise_f144(dat).value for dat in self.f144_data]
