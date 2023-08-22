import os
import sys

current_directory = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.join(current_directory, '..')
alignment_engine_path = os.path.join(project_root, 'alignment_engine')
sys.path.append(alignment_engine_path)

import time

import pytest
import pykka

from unittest.mock import MagicMock, patch

from streaming_data_types import deserialise_f144, deserialise_x5f2

from alignment_engine.main.supervisors.producer_supervisor import ProducerSupervisorActor
from alignment_engine.main.actors.producer_actor import ProducerActor, ProducerLogic
from tests.doubles.producer import ProducerSpy

PLOT_MESSAGE = {
    'data': {
        'sender1': [0, 1, 2, 3, 4],
        'sender2': [0, 2, 4, 6, 8],
    }
}

FIT_MESSAGE = {
    'data': {
        'sender1': {
            'fit_function': 'gauss',
            'fit_params': [0, 100, 5, 1],
            'r_squared': 0.99,
        }
    }
}

STATUS_MESSAGE = {'status':
    [
        "TestSoftware",
        "1.0.0",
        "TestService",
        "TestHost",
        12345,
        1000,
        '{"status": "active"}',
    ]
}


class TestSupervisorActorAdvancedWorker:
    @pytest.fixture
    def supervisor(self):
        self.test_topic = 'test_topic'

        self.producer_spies = [
            ProducerSpy({}),
            ProducerSpy({}),
            ProducerSpy({}),
        ]

        supervisor = ProducerSupervisorActor.start(None, ProducerActor)
        self.producer_logics = [
            ProducerLogic(self.producer_spies[0], self.test_topic),
            ProducerLogic(self.producer_spies[1], self.test_topic),
            ProducerLogic(self.producer_spies[2], self.test_topic),
        ]
        yield supervisor
        pykka.ActorRegistry.stop_all()

    def test_spawn_advanced_worked(self, supervisor):
        supervisor.tell({'command': 'SPAWN', 'config': {'actor_configs': [
            {
                'producer_logic': self.producer_logics[0],
            },
            {
                'producer_logic': self.producer_logics[1],
            },
            {
                'producer_logic': self.producer_logics[2],
            }
        ]}})
        worker_statuses = supervisor.ask({'command': 'STATUS'})
        assert len(worker_statuses['worker_statuses']) == 3

    def test_advanced_workers_args_works(self, supervisor):
        supervisor.tell({'command': 'SPAWN', 'config': {'actor_configs': [
            {
                'producer_logic': self.producer_logics[0],
            },
            {
                'producer_logic': self.producer_logics[1],
            },
            {
                'producer_logic': self.producer_logics[2],
            }
        ]}})

        for producer, msg in zip(self.producer_logics, [PLOT_MESSAGE, FIT_MESSAGE, STATUS_MESSAGE]):
            producer.produce_message(msg)

        time.sleep(0.05)

        assert deserialise_f144(self.producer_spies[0].data[0]['value']).value.tolist() == PLOT_MESSAGE['data']['sender1']
        assert deserialise_f144(self.producer_spies[0].data[1]['value']).value.tolist() == PLOT_MESSAGE['data']['sender2']
        assert deserialise_f144(self.producer_spies[1].data[-1]['value']).value.tolist() == FIT_MESSAGE['data']['sender1']['fit_params']
        assert deserialise_x5f2(self.producer_spies[2].data[-1]['value']).process_id == 12345

