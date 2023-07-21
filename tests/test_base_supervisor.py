import time
from unittest.mock import MagicMock

import pytest
import pykka

from src.main.supervisors.base_supervisor import BaseSupervisorActor
from tests.doubles.workers import TestWorkerActor, TestWorkerActor2


class TestSupervisorActorSimpleWorker:
    @pytest.fixture
    def supervisor(self):
        supervisor = BaseSupervisorActor.start(TestWorkerActor)
        yield supervisor
        pykka.ActorRegistry.stop_all()

    def test_spawn(self, supervisor):
        supervisor.tell({'command': 'SPAWN', 'config': {'actor_configs': [{}]}})
        worker_statuses = supervisor.ask({'command': 'STATUS'})
        assert len(worker_statuses['worker_statuses']) == 1

        supervisor.tell({'command': 'SPAWN', 'config': {'actor_configs': [{}]}})
        worker_statuses = supervisor.ask({'command': 'STATUS'})
        assert len(worker_statuses['worker_statuses']) == 2

    def test_status(self, supervisor):
        supervisor.tell({'command': 'SPAWN', 'config': {'actor_configs': [{}]}})
        worker_statuses = supervisor.ask({'command': 'STATUS'})
        assert worker_statuses['status'] == 'BaseSupervisorActor is alive'
        assert len(worker_statuses['worker_statuses']) == 1

    def test_register(self, supervisor):
        supervisor.tell({'command': 'SPAWN', 'config': {'actor_configs': [{}]}})
        worker_statuses = supervisor.ask({'command': 'STATUS'})
        assert len(worker_statuses['worker_statuses']) == 1

    def test_failed(self, supervisor):
        supervisor.tell({'command': 'SPAWN', 'config': {'actor_configs': [{}]}})
        worker_urn = list(supervisor.ask({'command': 'STATUS'})['worker_statuses'].keys())[0]
        worker_ref = pykka.ActorRegistry.get_by_urn(worker_urn)
        worker_ref.tell({'command': 'FAIL'})
        time.sleep(0.01)
        new_worker_statuses = supervisor.ask({'command': 'STATUS'})
        assert len(new_worker_statuses['worker_statuses']) == 1
        assert worker_urn not in new_worker_statuses['worker_statuses']

    def test_multiple_worker_registration(self, supervisor):
        supervisor.tell({'command': 'SPAWN', 'config': {'actor_configs': [{} for _ in range(5)]}})
        worker_statuses = supervisor.ask({'command': 'STATUS'})
        assert len(worker_statuses['worker_statuses']) == 5

    def test_failed_with_multiple_workers(self, supervisor):
        supervisor.tell({'command': 'SPAWN', 'config': {'actor_configs': [{} for _ in range(5)]}})
        worker_urns = list(supervisor.ask({'command': 'STATUS'})['worker_statuses'].keys())
        for worker_urn in worker_urns:
            worker_ref = pykka.ActorRegistry.get_by_urn(worker_urn)
            worker_ref.tell({'command': 'FAIL'})
            time.sleep(0.01)
        new_worker_statuses = supervisor.ask({'command': 'STATUS'})
        assert len(new_worker_statuses['worker_statuses']) == 5
        for worker_urn in worker_urns:
            assert worker_urn not in new_worker_statuses['worker_statuses']

    def test_spawn_with_no_config(self, supervisor):
        with pytest.raises(ValueError):
            supervisor.ask({'command': 'SPAWN'}).get()

    def test_unregister_unknown_worker(self, supervisor):
        actor_mock = MagicMock()
        actor_mock.actor_urn = 'urn:uuid:unknown-urn'
        supervisor.tell({'command': 'FAILED', 'actor': actor_mock})
        # Check that it didn't crash
        supervisor.ask({'command': 'STATUS'})


class TestSupervisorActorAdvancedWorker:
    @pytest.fixture
    def supervisor(self):
        supervisor = BaseSupervisorActor.start(TestWorkerActor2)
        self.some_other_actor_mock = MagicMock()
        self.some_logic_class_mock = MagicMock()
        yield supervisor
        pykka.ActorRegistry.stop_all()

    def test_spawn_advanced_worked(self, supervisor):
        supervisor.tell({'command': 'SPAWN', 'config': {'actor_configs': [{
            'some_other_actor': self.some_other_actor_mock,
            'some_logic_class': self.some_logic_class_mock,
        }]}})
        worker_statuses = supervisor.ask({'command': 'STATUS'})
        assert len(worker_statuses['worker_statuses']) == 1

    def test_advanced_workers_args_works(self, supervisor):
        supervisor.tell({'command': 'SPAWN', 'config': {'actor_configs': [{
            'some_other_actor': self.some_other_actor_mock,
            'some_logic_class': self.some_logic_class_mock,
        }]}})
        worker_urns = list(supervisor.ask({'command': 'STATUS'})['worker_statuses'].keys())
        for worker_urn in worker_urns:
            worker_proxy = pykka.ActorRegistry.get_by_urn(worker_urn).proxy()
            worker_proxy.some_other_actor().get()
            worker_proxy.some_logic_class().get()
            self.some_other_actor_mock.assert_called_once()
            self.some_other_actor_mock.reset_mock()
            self.some_logic_class_mock.assert_called_once()
            self.some_logic_class_mock.reset_mock()
