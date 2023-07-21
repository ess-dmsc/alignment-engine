import time

import pytest
import pykka

from unittest.mock import MagicMock, patch
from src.main.supervisors.consumer_supervisor import ConsumerSupervisorActor
from src.main.actors.consumer_actor import ConsumerActor, ConsumerLogic
from tests.doubles.consumer import ConsumerStub
from tests.test_consumer_actor import generate_fake_f144_data


class TestSupervisorActorAdvancedWorker:
    @pytest.fixture
    def supervisor(self):
        f144_data_1 = generate_fake_f144_data(10, source_name="f144_source_1")
        f144_data_2 = generate_fake_f144_data(10, source_name="f144_source_2")
        f144_data = f144_data_1 + f144_data_2

        consumer_stub_1 = ConsumerStub({})
        consumer_stub_2 = ConsumerStub({})
        for i, data in enumerate(f144_data):
            consumer_stub_1.add_message('topic', 0, i + 1, None, data)
            consumer_stub_2.add_message('topic', 0, i + 1, None, data)

        supervisor = ConsumerSupervisorActor.start(None, ConsumerActor)
        self.consumer_logics = [
            ConsumerLogic(consumer_stub_1, source_name='f144_source_1'),
            ConsumerLogic(consumer_stub_2, source_name='f144_source_2')
        ]
        self.data_handler_actor_mocks = [
            MagicMock(),
            MagicMock()
        ]
        yield supervisor
        pykka.ActorRegistry.stop_all()

    def test_spawn_advanced_worked(self, supervisor):
        supervisor.tell({'command': 'SPAWN', 'config': {'actor_configs': [
            {
                'data_handler_actor': self.data_handler_actor_mocks[0],
                'consumer_logic': self.consumer_logics[0],
            },
            {
                'data_handler_actor': self.data_handler_actor_mocks[1],
                'consumer_logic': self.consumer_logics[1],
            }
        ]}})
        worker_statuses = supervisor.ask({'command': 'STATUS'})
        assert len(worker_statuses['worker_statuses']) == 2

    def test_advanced_workers_args_works(self, supervisor):
        supervisor.tell({'command': 'SPAWN', 'config': {'actor_configs': [
            {
                'data_handler_actor': self.data_handler_actor_mocks[0],
                'consumer_logic': self.consumer_logics[0],
            },
            {
                'data_handler_actor': self.data_handler_actor_mocks[1],
                'consumer_logic': self.consumer_logics[1],
            }
        ]}})

        # start consuming
        worker_urns = list(supervisor.ask({'command': 'STATUS'})['worker_statuses'].keys())
        worker_refs = [pykka.ActorRegistry.get_by_urn(worker_urn) for worker_urn in worker_urns]
        for worker_ref in worker_refs:
            worker_ref.tell({'command': 'START'})

        time.sleep(0.1)

        for worker_ref in worker_refs:
            worker_ref.tell({'command': 'STOP'})

        assert self.data_handler_actor_mocks[0].tell.call_count == 10
        assert self.data_handler_actor_mocks[1].tell.call_count == 10

    def test_advanced_workers_killed_during_consumption(self, supervisor):
        self.data_handler_actor_mocks[0].tell.side_effect = lambda x: time.sleep(0.01)
        supervisor.tell({'command': 'SPAWN', 'config': {'actor_configs': [
            {
                'data_handler_actor': self.data_handler_actor_mocks[0],
                'consumer_logic': self.consumer_logics[0],
            },
            {
                'data_handler_actor': self.data_handler_actor_mocks[1],
                'consumer_logic': self.consumer_logics[1],
            }
        ]}})

        # start consuming
        worker_urns = list(supervisor.ask({'command': 'STATUS'})['worker_statuses'].keys())
        worker_refs = [pykka.ActorRegistry.get_by_urn(worker_urn) for worker_urn in worker_urns]
        for worker_ref in worker_refs:
            worker_ref.tell({'command': 'START'})

        time.sleep(0.05)

        supervisor.tell({'command': 'KILL', 'actor_urn': worker_urns[0]})

        new_worker_statuses = supervisor.ask({'command': 'STATUS'})
        assert len(new_worker_statuses['worker_statuses']) == 1

        worker_refs[1].tell({'command': 'STOP'})

        assert self.data_handler_actor_mocks[0].tell.call_count in [4, 5, 6]
        assert self.data_handler_actor_mocks[1].tell.call_count == 10

    def test_worker_exception_during_consumption(self, supervisor):
        # Mock `consume_message` with our side effect
        with patch.object(self.consumer_logics[0], 'consume_message', side_effect=Exception('Test exception')):
            # Spawn workers
            supervisor.tell({'command': 'SPAWN', 'config': {'actor_configs': [
                {
                    'data_handler_actor': self.data_handler_actor_mocks[0],
                    'consumer_logic': self.consumer_logics[0],
                }
            ]}})

            # Start consuming
            worker_urn = list(supervisor.ask({'command': 'STATUS'})['worker_statuses'].keys())[0]
            worker_ref = pykka.ActorRegistry.get_by_urn(worker_urn)
            worker_ref.tell({'command': 'START'})

            # Give some time for consumption to occur
            time.sleep(0.01)

            # The worker must have crashed and then restarted
            new_worker_statuses = supervisor.ask({'command': 'STATUS'})
            assert len(new_worker_statuses['worker_statuses']) == 1
            assert worker_urn not in new_worker_statuses['worker_statuses']
            assert self.data_handler_actor_mocks[0].tell.call_count == 0



