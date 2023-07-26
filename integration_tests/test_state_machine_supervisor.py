import json
import time

import numpy as np
import pytest
import pykka
from streaming_data_types import deserialise_f144

from src.main.actors.consumer_actor import ConsumerActor
from src.main.actors.data_handler_actor import DataHandlerActor
from src.main.actors.producer_actor import ProducerActor, ProducerLogic
from src.main.supervisors.consumer_supervisor import ConsumerSupervisorActor
from src.main.supervisors.producer_supervisor import ProducerSupervisorActor
from src.main.supervisors.state_machine_supervisor import StateMachineSupervisorActor
from src.main.supervisors.data_handler_supervisor import DataHandlerSupervisorActor
from src.main.actors.interpolator_actor import InterpolatorActor, InterpolatorLogic
from src.main.actors.fitter_actor import FitterActor, FitterLogic
from src.main.actors.commander_actor import CommanderActor, CommanderLogic
from tests.doubles.consumer import ConsumerStub
from tests.doubles.producer import ProducerSpy

from integration_tests.test_actor_data_pipeline import generate_gauss_f144_data, generate_gauss_ev44_events


CONFIG = {
    'stream_configs': {
        'device1': {
            'broker': 'localhost:9092',
            'topic': 'device1_topic',
            'source': 'f144_source_1',
            'is_control': True,
        },
        'device2': {
            'broker': 'localhost:9092',
            'topic': 'device2_topic',
            'source': 'ev44_source_1',
            'is_control': False,
        },
    },
    'fitter_config': {
        'control_signals': ['f144_source_1'],
        'readout_signals': ['ev44_source_1'],
        'fit_function': 'gauss',
    }
}


class ConsumerFactoryStub:
    def __init__(self):
        self.consumers = [ConsumerStub({}), ConsumerStub({})]
        f144_data = generate_gauss_f144_data(101)
        ev44_data = generate_gauss_ev44_events(201)
        for i, data in enumerate(f144_data):
            self.consumers[0].add_message('topic', 0, i + 1, None, data)
        for i, data in enumerate(ev44_data):
            self.consumers[1].add_message('topic', 0, i + 1, None, data)

    def create_consumer(self, broker, topic, source):
        return self.consumers.pop(0)


class ProducerFactoryStub:
    def __init__(self):
        self.producers = [ProducerSpy({}), ProducerSpy({}), ProducerSpy({})]

    def create_producer(self, broker, topic):
        return self.producers.pop(0)


class TestStateMachineSupervisorActor:
    @pytest.fixture
    def supervisor(self):
        supervisor = StateMachineSupervisorActor.start(
            None,
            [
                ConsumerSupervisorActor,
                ProducerSupervisorActor,
                DataHandlerSupervisorActor,
                InterpolatorActor,
                FitterActor,
                CommanderActor,
            ],
            ConsumerFactoryStub(),
            ProducerFactoryStub(),
        )

        self.commander_consumer = ConsumerStub({})
        self.commander_consumer.add_message('command_topic', 0, 1, None,
                                            ('{"command": "CONFIG", "config": ' + json.dumps(CONFIG) + '}').encode(
                                                'utf-8')
                                            )
        self.commander_consumer.add_message('command_topic', 0, 2, None, '{"command": "START"}'.encode('utf-8'))


        yield supervisor
        pykka.ActorRegistry.stop_all()

    def test_spawn_all_workers_within_statemachine(self, supervisor):
        time.sleep(0.5)
        worker_statuses = supervisor.ask({'command': 'STATUS'})
        assert len(worker_statuses['worker_statuses']) == 6

    def test_spawn_set_logic_on_commander(self, supervisor):
        commander_logic = CommanderLogic(self.commander_consumer)
        commander_actor = supervisor.proxy().workers_by_type.get()['CommanderActor']
        commander_actor.ask({'command': 'SET_LOGIC', 'logic': commander_logic})

        commander_logic_readback = commander_actor.proxy().commander_logic.get()
        assert commander_logic_readback == commander_logic
        assert commander_actor.is_alive()

    def test_spawn_producers_and_set_logic(self, supervisor):
        producer_spies = [
            ProducerSpy({}),
            ProducerSpy({}),
            ProducerSpy({}),
        ]
        producer_logics = [
            ProducerLogic(producer_spies[0], 'output_topic'),
            ProducerLogic(producer_spies[1], 'output_topic'),
            ProducerLogic(producer_spies[2], 'output_topic'),
        ]
        producer_supervisor = supervisor.proxy().workers_by_type.get()['ProducerSupervisorActor']
        producer_supervisor.ask({'command': 'SPAWN', 'config': {
            'actor_configs': [
                {},
                {},
                {},
            ]
        }})

        assert len(producer_supervisor.ask({'command': 'STATUS'})['worker_statuses']) == 3

        producer_actors_urns = list(producer_supervisor.ask({'command': 'STATUS'})['worker_statuses'].keys())
        producer_actors = [pykka.ActorRegistry.get_by_urn(urn) for urn in producer_actors_urns]
        for i, producer_actor in enumerate(producer_actors):
            producer_actor.ask({'command': 'SET_LOGIC', 'logic': producer_logics[i]})

        assert producer_actors[0].proxy().producer_logic.get() == producer_logics[0]
        assert producer_actors[1].proxy().producer_logic.get() == producer_logics[1]
        assert producer_actors[2].proxy().producer_logic.get() == producer_logics[2]

    def test_spawn_fitter(self, supervisor):
        supervisor.ask({'command': 'SPAWN', 'config': {
            'FitterActor': {
                'actor_configs': [
                    {}
                ]
            }
        }})

        fitter_actor = supervisor.proxy().workers_by_type.get()['FitterActor']
        assert fitter_actor.is_alive()

    def test_spawn_interpolator(self, supervisor):
        supervisor.ask({'command': 'SPAWN', 'config': {
            'InterpolatorActor': {
                'actor_configs': [
                    {}
                ]
            }
        }})

        interpolator_actor = supervisor.proxy().workers_by_type.get()['InterpolatorActor']
        assert interpolator_actor.is_alive()

    def test_spawn_data_handlers(self, supervisor):
        data_handler_supervisor = supervisor.proxy().workers_by_type.get()['DataHandlerSupervisorActor']

        data_handler_supervisor.ask({'command': 'SPAWN', 'config': {
            'actor_configs': [
                {},
                {},
            ]
        }})

        assert len(data_handler_supervisor.ask({'command': 'STATUS'})['worker_statuses']) == 2
        assert data_handler_supervisor.is_alive()

    def test_spawn_consumers(self, supervisor):
        consumer_supervisor = supervisor.proxy().workers_by_type.get()['ConsumerSupervisorActor']

        consumer_supervisor.ask({'command': 'SPAWN', 'config': {
            'actor_configs': [
                {},
                {},
            ]
        }})

        assert len(consumer_supervisor.ask({'command': 'STATUS'})['worker_statuses']) == 2
        assert consumer_supervisor.is_alive()

    def test_spawn_all_workers_from_commander(self, supervisor):
        commander_logic = CommanderLogic(self.commander_consumer)
        commander_actor = supervisor.proxy().workers_by_type.get()['CommanderActor']
        commander_actor.ask({'command': 'SET_LOGIC', 'logic': commander_logic})

        time.sleep(0.1)
        commander_actor.ask({'command': 'START'})
        time.sleep(0.5)
        commander_actor.ask({'command': 'STOP'})

        assert supervisor.is_alive()
        assert commander_actor.is_alive()

        assert len(supervisor.ask({'command': 'STATUS'})['worker_statuses']) == 6

        producer_supervisor = supervisor.proxy().workers_by_type.get()['ProducerSupervisorActor']
        assert len(producer_supervisor.ask({'command': 'STATUS'})['worker_statuses']) == 3

        data_handler_supervisor = supervisor.proxy().workers_by_type.get()['DataHandlerSupervisorActor']
        assert len(data_handler_supervisor.ask({'command': 'STATUS'})['worker_statuses']) == 2

        consumer_supervisor = supervisor.proxy().workers_by_type.get()['ConsumerSupervisorActor']
        assert len(consumer_supervisor.ask({'command': 'STATUS'})['worker_statuses']) == 2

        assert supervisor.is_alive()

    def test_spawn_all_workers_and_set_logic_from_commander(self, supervisor):
        commander_logic = CommanderLogic(self.commander_consumer)
        commander_actor = supervisor.proxy().workers_by_type.get()['CommanderActor']
        commander_actor.ask({'command': 'SET_LOGIC', 'logic': commander_logic})

        time.sleep(0.1)
        commander_actor.ask({'command': 'START'})
        time.sleep(0.5)
        commander_actor.ask({'command': 'STOP'})

        worker_by_type = supervisor.proxy().workers_by_type.get()
        producer_supervisor = worker_by_type['ProducerSupervisorActor']
        datahandler_supervisor = worker_by_type['DataHandlerSupervisorActor']
        consumer_supervisor = worker_by_type['ConsumerSupervisorActor']
        interpolator_actor = worker_by_type['InterpolatorActor']
        fitter_actor = worker_by_type['FitterActor']

        producer_urns = list(producer_supervisor.ask({'command': 'STATUS'})['worker_statuses'].keys())
        producer_actors = [pykka.ActorRegistry.get_by_urn(urn) for urn in producer_urns]
        datahandler_urns = list(datahandler_supervisor.ask({'command': 'STATUS'})['worker_statuses'].keys())
        datahandler_actors = [pykka.ActorRegistry.get_by_urn(urn) for urn in datahandler_urns]
        consumer_urns = list(consumer_supervisor.ask({'command': 'STATUS'})['worker_statuses'].keys())
        consumer_actors = [pykka.ActorRegistry.get_by_urn(urn) for urn in consumer_urns]

        assert len(producer_actors) == 3
        assert len(datahandler_actors) == 2
        assert len(consumer_actors) == 2

        interpolator_logic_readback = interpolator_actor.proxy().interpolator_logic.get()
        fitter_logic_readback = fitter_actor.proxy().fitter_logic.get()
        datahandler_logic_readbacks = [
            datahandler_actor.proxy().data_handler_logic.get() for datahandler_actor in datahandler_actors
        ]
        producer_logic_readbacks = [
            producer_actor.proxy().producer_logic.get() for producer_actor in producer_actors
        ]
        consumer_logic_readbacks = [
            consumer_actor.proxy().consumer_logic.get() for consumer_actor in consumer_actors
        ]

        assert all(producer_logic_readbacks)
        assert fitter_logic_readback is not None
        assert interpolator_logic_readback is not None
        assert all(datahandler_logic_readbacks)
        assert all(consumer_logic_readbacks)

    def test_spawn_all_workers_and_set_connections_from_commander(self, supervisor):
        commander_logic = CommanderLogic(self.commander_consumer)
        commander_actor = supervisor.proxy().workers_by_type.get()['CommanderActor']
        commander_actor.ask({'command': 'SET_LOGIC', 'logic': commander_logic})

        time.sleep(0.1)
        commander_actor.ask({'command': 'START'})
        time.sleep(0.5)
        commander_actor.ask({'command': 'STOP'})

        worker_by_type = supervisor.proxy().workers_by_type.get()
        producer_supervisor = worker_by_type['ProducerSupervisorActor']
        datahandler_supervisor = worker_by_type['DataHandlerSupervisorActor']
        consumer_supervisor = worker_by_type['ConsumerSupervisorActor']
        interpolator_actor = worker_by_type['InterpolatorActor']
        fitter_actor = worker_by_type['FitterActor']

        producer_urns = list(producer_supervisor.ask({'command': 'STATUS'})['worker_statuses'].keys())
        producer_actors = [pykka.ActorRegistry.get_by_urn(urn) for urn in producer_urns]
        datahandler_urns = list(datahandler_supervisor.ask({'command': 'STATUS'})['worker_statuses'].keys())
        datahandler_actors = [pykka.ActorRegistry.get_by_urn(urn) for urn in datahandler_urns]
        consumer_urns = list(consumer_supervisor.ask({'command': 'STATUS'})['worker_statuses'].keys())
        consumer_actors = [pykka.ActorRegistry.get_by_urn(urn) for urn in consumer_urns]

        assert len(producer_actors) == 3
        assert len(datahandler_actors) == 2
        assert len(consumer_actors) == 2

        fitter_producer_readback = fitter_actor.proxy().producer_actor.get()
        interpolator_producer_readback = interpolator_actor.proxy().producer_actor.get()
        interpolator_fitter_readback = interpolator_actor.proxy().fitter_actor.get()
        datahandler_interpolator_readbacks = [
            datahandler_actor.proxy().interpolator_actor.get() for datahandler_actor in datahandler_actors
        ]
        consumer_datahandler_readbacks = [
            consumer_actor.proxy().data_handler_actor.get() for consumer_actor in consumer_actors
        ]

        assert fitter_producer_readback is not None
        assert interpolator_producer_readback is not None
        assert interpolator_fitter_readback is not None
        assert all(datahandler_interpolator_readbacks)
        assert all(consumer_datahandler_readbacks)

    def test_spawn_all_workers_and_run_data_through_pipeline(self, supervisor):
        commander_logic = CommanderLogic(self.commander_consumer)
        commander_actor = supervisor.proxy().workers_by_type.get()['CommanderActor']
        commander_actor.ask({'command': 'SET_LOGIC', 'logic': commander_logic})

        time.sleep(0.1)
        commander_actor.ask({'command': 'START'})
        time.sleep(0.5)
        commander_actor.ask({'command': 'STOP'})

        worker_by_type = supervisor.proxy().workers_by_type.get()
        producer_supervisor = worker_by_type['ProducerSupervisorActor']
        datahandler_supervisor = worker_by_type['DataHandlerSupervisorActor']
        consumer_supervisor = worker_by_type['ConsumerSupervisorActor']
        interpolator_actor = worker_by_type['InterpolatorActor']
        fitter_actor = worker_by_type['FitterActor']

        producer_urns = list(producer_supervisor.ask({'command': 'STATUS'})['worker_statuses'].keys())
        producer_actors = [pykka.ActorRegistry.get_by_urn(urn) for urn in producer_urns]
        datahandler_urns = list(datahandler_supervisor.ask({'command': 'STATUS'})['worker_statuses'].keys())
        datahandler_actors = [pykka.ActorRegistry.get_by_urn(urn) for urn in datahandler_urns]
        consumer_urns = list(consumer_supervisor.ask({'command': 'STATUS'})['worker_statuses'].keys())
        consumer_actors = [pykka.ActorRegistry.get_by_urn(urn) for urn in consumer_urns]

        assert len(producer_actors) == 3
        assert len(datahandler_actors) == 2
        assert len(consumer_actors) == 2

        for consumer_actor in consumer_actors:
            consumer_actor.tell({'command': 'START'})

        time.sleep(0.5)

        for consumer_actor in consumer_actors:
            consumer_actor.tell({'command': 'STOP'})

        time.sleep(0.5)

        producer_spy_fitt = supervisor.proxy().producers.get()[0]
        producer_spy_inte = supervisor.proxy().producers.get()[1]

        received_data_from_fitter_producer = deserialise_f144(producer_spy_fitt.data[-1]['value'])
        received_data_from_interpolator_producer = [deserialise_f144(dat['value']) for dat in producer_spy_inte.data]

        reduced_interpolator_data = {}
        for dat in received_data_from_interpolator_producer:
            reduced_interpolator_data[dat.source_name] = dat.value

        computed_fit_params = received_data_from_fitter_producer.value
        optima = computed_fit_params[2]
        ev44_source_1_data = reduced_interpolator_data['ev44_source_1']
        f144_source_1_data = reduced_interpolator_data['f144_source_1']

        # import matplotlib.pyplot as plt
        # plt.plot(f144_source_1_data, ev44_source_1_data, 'g', label='data')
        # plt.vlines(optima, 80, 120, label='optima', linewidth=3)
        # plt.show()

        assert np.allclose(optima, 5., atol=0.1)
        assert len(ev44_source_1_data) == len(f144_source_1_data)
