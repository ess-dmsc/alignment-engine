import time

import pytest
import pykka

from unittest.mock import MagicMock, patch
from src.main.supervisors.data_handler_supervisor import DataHandlerSupervisorActor
from src.main.actors.data_handler_actor import DataHandlerActor, DataHandlerLogic

from tests.test_data_handler_actor import generate_fake_f144_data, generate_fake_ev44_events


class TestSupervisorActorAdvancedWorker:
    @pytest.fixture
    def supervisor(self):
        supervisor = DataHandlerSupervisorActor.start(None, DataHandlerActor)
        self.interpolator_actor_mocks = [
            MagicMock(),
            MagicMock()
        ]
        self.data_handler_logics = [
            DataHandlerLogic(),
            DataHandlerLogic()
        ]
        yield supervisor
        pykka.ActorRegistry.stop_all()

    def test_spawn_advanced_worked(self, supervisor):
        supervisor.tell({'command': 'SPAWN', 'config': {'actor_configs': [
            {
                'interpolator_actor': self.interpolator_actor_mocks[0],
                'data_handler_logic': self.data_handler_logics[0],
            },
            {
                'interpolator_actor': self.interpolator_actor_mocks[1],
                'data_handler_logic': self.data_handler_logics[1],
            }
        ]}})
        worker_statuses = supervisor.ask({'command': 'STATUS'})
        assert len(worker_statuses['worker_statuses']) == 2

    def test_advanced_workers_args_works(self, supervisor):
        supervisor.tell({'command': 'SPAWN', 'config': {'actor_configs': [
            {
                'interpolator_actor': self.interpolator_actor_mocks[0],
                'data_handler_logic': self.data_handler_logics[0],
            },
            {
                'interpolator_actor': self.interpolator_actor_mocks[1],
                'data_handler_logic': self.data_handler_logics[1],
            }
        ]}})

        # start consuming
        worker_urns = list(supervisor.ask({'command': 'STATUS'})['worker_statuses'].keys())
        worker_refs = [pykka.ActorRegistry.get_by_urn(worker_urn) for worker_urn in worker_urns]
        for worker_ref in worker_refs:
            fake_data = generate_fake_ev44_events(1)[0]
            worker_ref.tell({'data': fake_data})

        time.sleep(0.05)

        for interpolator_actor_mock in self.interpolator_actor_mocks:
            interpolator_actor_mock.tell.assert_called_once()

    def test_advanced_workers_killed_during_consumption(self, supervisor):
        supervisor.tell({'command': 'SPAWN', 'config': {'actor_configs': [
            {
                'interpolator_actor': self.interpolator_actor_mocks[0],
                'data_handler_logic': self.data_handler_logics[0],
            },
            {
                'interpolator_actor': self.interpolator_actor_mocks[1],
                'data_handler_logic': self.data_handler_logics[1],
            }
        ]}})

        # start consuming
        worker_urns = list(supervisor.ask({'command': 'STATUS'})['worker_statuses'].keys())
        worker_refs = [pykka.ActorRegistry.get_by_urn(worker_urn) for worker_urn in worker_urns]
        for worker_ref in worker_refs:
            fake_data = generate_fake_ev44_events(1)[0]
            worker_ref.tell({'data': fake_data})

        time.sleep(0.05)

        for interpolator_actor_mock in self.interpolator_actor_mocks:
            interpolator_actor_mock.tell.assert_called_once()

        supervisor.tell({'command': 'KILL', 'actor_urn': worker_urns[0]})

        time.sleep(0.05)

        new_worker_statuses = supervisor.ask({'command': 'STATUS'})
        assert len(new_worker_statuses['worker_statuses']) == 1

        # start consuming
        worker_urns = list(new_worker_statuses['worker_statuses'].keys())
        worker_refs = [pykka.ActorRegistry.get_by_urn(worker_urn) for worker_urn in worker_urns]
        for worker_ref in worker_refs:
            fake_data = generate_fake_ev44_events(1)[0]
            worker_ref.tell({'data': fake_data})

        time.sleep(0.05)

        assert self.interpolator_actor_mocks[0].tell.call_count == 1
        assert self.interpolator_actor_mocks[1].tell.call_count == 2

    def test_worker_exception_during_consumption(self, supervisor):
        # Mock `consume_message` with our side effect
        with patch.object(self.data_handler_logics[0], 'start', side_effect=Exception('Test exception')):
            # Spawn workers
            supervisor.tell({'command': 'SPAWN', 'config': {'actor_configs': [
                {
                    'interpolator_actor': self.interpolator_actor_mocks[0],
                    'data_handler_logic': self.data_handler_logics[0],
                }
            ]}})

            # Start consuming
            worker_urn = list(supervisor.ask({'command': 'STATUS'})['worker_statuses'].keys())[0]
            worker_ref = pykka.ActorRegistry.get_by_urn(worker_urn)
            worker_ref.tell({'command': 'START'})
            fake_data = generate_fake_ev44_events(1)[0]
            worker_ref.tell({'data': fake_data})

            # Give some time for consumption to occur
            time.sleep(0.05)

            # The worker must have crashed and then restarted
            new_worker_statuses = supervisor.ask({'command': 'STATUS'})
            assert len(new_worker_statuses['worker_statuses']) == 1
            assert worker_urn not in new_worker_statuses['worker_statuses']
            assert self.interpolator_actor_mocks[0].tell.call_count == 0



