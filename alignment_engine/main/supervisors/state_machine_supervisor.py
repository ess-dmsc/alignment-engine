import pykka

from main.actors.consumer_actor import ConsumerActor, ConsumerLogic
from main.actors.data_handler_actor import DataHandlerActor, DataHandlerLogic
from main.actors.fitter_actor import FitterLogic
from main.actors.interpolator_actor import InterpolatorLogic
from main.actors.producer_actor import ProducerActor, ProducerLogic


class StateMachineSupervisorActor(pykka.ThreadingActor):
    def __init__(self, supervisor, worker_classes, consumer_factory, producer_factory):
        super().__init__()
        self.worker_classes = {
            worker_class.__name__: worker_class for worker_class in worker_classes
        }
        self.supervisor = supervisor
        self.workers = {}
        self.workers_configs = {}
        self.workers_by_type = {}

        self.consumer_factory = consumer_factory
        self.producer_factory = producer_factory

        self.producers = [self.producer_factory.create_producer(broker='localhost:9092') for _ in range(3)]

    def on_start(self):
        # print(f"Starting {self.__class__.__name__}")
        self.spawn_base_system()
        if self.supervisor is None:
            return
        self.supervisor.tell({'command': 'REGISTER', 'actor': self.actor_ref})

    def on_failure(self, exception_type, exception_value, traceback):
        print(f"Supervisor {self.__class__.__name__} has failed with exception {exception_type}, {exception_value}, {traceback}")
        if self.supervisor is None:
            return
        self.supervisor.tell({'command': 'FAILED', 'actor': self.actor_ref})

    def on_receive(self, message):
        command = message.get('command')

        if command == 'REGISTER':
            actor = message.get('actor')
            self.workers[actor.actor_urn] = actor

        elif command == 'FAILED':
            actor = message.get('actor')
            last_config = message.get('last_config', None)
            actor_class_name = message.get('actor_class_name')
            if actor.actor_urn not in self.workers:
                print(f"{actor.__class__.__name__} {actor.actor_urn} has died. Not this supervisor's worker")
                return
            print(f"{actor.__class__.__name__} {actor.actor_urn} has died. Restarting...")
            worker_class = self.worker_classes.get(actor_class_name, None)
            if last_config is not None:
                self.workers_configs[actor.actor_urn] = last_config
            actor_config = self.workers_configs[actor.actor_urn]
            new_actor = worker_class.start(self.actor_ref, **actor_config)
            print(f"New {new_actor.__class__.__name__} {new_actor.actor_urn} has been started")
            del self.workers[actor.actor_urn]
            self.workers[new_actor.actor_urn] = new_actor
            self.workers_by_type[actor_class_name] = new_actor
            # self.relink_workers(actor_class_name)

        elif command == 'SPAWN':
            config = message.get('config', None)
            if config is None:
                raise ValueError("No config provided for SPAWN command")

            for worker_class_name, class_configs in config.items():
                worker_class = self.worker_classes.get(worker_class_name, None)
                if worker_class is None:
                    continue
                for actor_config in class_configs['actor_configs']:
                    # print(f"Spawning {worker_class_name} with config {actor_config}")
                    actor = worker_class.start(self.actor_ref, **actor_config)
                    self.workers[actor.actor_urn] = actor
                    self.workers_by_type[worker_class_name] = actor
                    self.workers_configs[actor.actor_urn] = actor_config

        elif command == 'KILL':
            actor_urn = message.get('actor_urn', None)
            if actor_urn is None:
                raise ValueError("No actor_urn provided for KILL command")
            if actor_urn not in self.workers:
                raise ValueError(f"Actor {actor_urn} is not a worker of this supervisor")
            self.workers[actor_urn].stop()
            del self.workers[actor_urn]
            del self.workers_configs[actor_urn]

        elif command == 'STATUS':
            statuses = {actor_urn: actor.ask({'command': 'STATUS'}) for actor_urn, actor in self.workers.items()}
            return {"status": f"{self.__class__.__name__} is alive", "worker_statuses": statuses}

        data = message.get('data', None)

        if data is not None:
            print(data)
            self.handle_external_command(data)

    def handle_external_command(self, data):
        command = data.get('command', None)

        print(f"Received command {command}")

        if command == 'CONFIG':
            config = data.get('config', None)
            if config is None:
                raise ValueError("No config provided for CONFIG command")

            num_workers = len(config.get('stream_configs', []))
            self.spawn_producer_supervisor()
            self.spawn_datahandler_supervisor(num_workers)
            self.spawn_consumer_supervisor(num_workers)

            self.configure_producer_supervisor(config)
            self.configure_fitter(config)
            self.configure_interpolator(config)
            self.configure_datahandler_supervisor(config)
            self.configure_consumer_supervisor(config)

            self.link_fitter_to_producer()
            self.link_interpolator_to_producer()
            self.link_interpolator_to_fitter()
            self.link_datahandlers_to_interpolator()
            self.link_consumers_to_datahandlers()

        elif command == 'START':
            consumer_supervisor = self.workers_by_type['ConsumerSupervisorActor']
            consumer_actor_urns = list(consumer_supervisor.ask({'command': 'STATUS'})['worker_statuses'].keys())
            consumer_actors = [pykka.ActorRegistry.get_by_urn(urn) for urn in consumer_actor_urns]
            for consumer_actor in consumer_actors:
                consumer_actor.tell({'command': 'START'})

        elif command == 'STOP':
            consumer_supervisor = self.workers_by_type['ConsumerSupervisorActor']
            consumer_actor_urns = list(consumer_supervisor.ask({'command': 'STATUS'})['worker_statuses'].keys())
            consumer_actors = [pykka.ActorRegistry.get_by_urn(urn) for urn in consumer_actor_urns]
            for consumer_actor in consumer_actors:
                consumer_actor.tell({'command': 'STOP'})


    def link_consumers_to_datahandlers(self):
        consumer_supervisor = self.workers_by_type['ConsumerSupervisorActor']
        consumer_actor_urns = list(consumer_supervisor.ask({'command': 'STATUS'})['worker_statuses'].keys())
        consumer_actors = [pykka.ActorRegistry.get_by_urn(urn) for urn in consumer_actor_urns]
        datahandler_supervisor = self.workers_by_type['DataHandlerSupervisorActor']
        datahandler_actor_urns = list(datahandler_supervisor.ask({'command': 'STATUS'})['worker_statuses'].keys())
        datahandler_actors = [pykka.ActorRegistry.get_by_urn(urn) for urn in datahandler_actor_urns]
        for consumer_actor, datahandler_actor in zip(consumer_actors, datahandler_actors):
            consumer_actor.tell({'command': 'SET_DATA_HANDLER_ACTOR', 'data_handler_actor': datahandler_actor})

    def link_datahandlers_to_interpolator(self):
        interpolator = self.workers_by_type['InterpolatorActor']
        datahandler_supervisor = self.workers_by_type['DataHandlerSupervisorActor']
        datahandler_actor_urns = list(datahandler_supervisor.ask({'command': 'STATUS'})['worker_statuses'].keys())
        datahandler_actors = [pykka.ActorRegistry.get_by_urn(urn) for urn in datahandler_actor_urns]
        for datahandler_actor in datahandler_actors:
            datahandler_actor.tell({'command': 'SET_INTERPOLATOR_ACTOR', 'interpolator_actor': interpolator})

    def link_interpolator_to_fitter(self):
        interpolator = self.workers_by_type['InterpolatorActor']
        fitter = self.workers_by_type['FitterActor']
        interpolator.tell({'command': 'SET_FITTER_ACTOR', 'fitter_actor': fitter})

    def link_interpolator_to_producer(self):
        producer_supervisor = self.workers_by_type['ProducerSupervisorActor']
        producer_actor_urns = list(producer_supervisor.ask({'command': 'STATUS'})['worker_statuses'].keys())
        producer_actors = [pykka.ActorRegistry.get_by_urn(urn) for urn in producer_actor_urns]
        interpolator_actor = self.workers_by_type['InterpolatorActor']
        interpolator_actor.tell({'command': 'SET_PRODUCER_ACTOR', 'producer_actor': producer_actors[1]})

    def link_fitter_to_producer(self):
        producer_supervisor = self.workers_by_type['ProducerSupervisorActor']
        producer_actor_urns = list(producer_supervisor.ask({'command': 'STATUS'})['worker_statuses'].keys())
        producer_actors = [pykka.ActorRegistry.get_by_urn(urn) for urn in producer_actor_urns]
        fitter_actor = self.workers_by_type['FitterActor']
        fitter_actor.tell({'command': 'SET_PRODUCER_ACTOR', 'producer_actor': producer_actors[0]})

    def configure_consumer_supervisor(self, config):
        consumer_supervisor = self.workers_by_type['ConsumerSupervisorActor']
        consumer_actor_urns = list(consumer_supervisor.ask({'command': 'STATUS'})['worker_statuses'].keys())
        consumer_actors = [pykka.ActorRegistry.get_by_urn(urn) for urn in consumer_actor_urns]
        source_names = [d['source'] for d in config['stream_configs'].values()]
        topic_names = [d['topic'] for d in config['stream_configs'].values()]
        consumers = [self.consumer_factory.create_consumer(broker='localhost:9092', topic=topic_names[i]) for i in range(len(consumer_actors))]
        consumer_logics = [ConsumerLogic(consumers[i], source_name=source_names[i]) for i in range(len(consumer_actors))]
        for consumer_actor, consumer_logic in zip(consumer_actors, consumer_logics):
            consumer_actor.tell({'command': 'SET_LOGIC', 'logic': consumer_logic})

    def configure_datahandler_supervisor(self, config):
        datahandler_supervisor = self.workers_by_type['DataHandlerSupervisorActor']
        datahandler_actor_urns = list(datahandler_supervisor.ask({'command': 'STATUS'})['worker_statuses'].keys())
        datahandler_actors = [pykka.ActorRegistry.get_by_urn(urn) for urn in datahandler_actor_urns]
        datahandler_logics = [DataHandlerLogic() for _ in range(len(datahandler_actors))]
        for datahandler_actor, datahandler_logic in zip(datahandler_actors, datahandler_logics):
            datahandler_actor.tell({'command': 'SET_LOGIC', 'logic': datahandler_logic})

    def configure_interpolator(self, config):
        interpolator = self.workers_by_type['InterpolatorActor']
        interpolator_logic = InterpolatorLogic()
        interpolator.tell({'command': 'SET_LOGIC', 'logic': interpolator_logic})

    def configure_fitter(self, config):
        fitter = self.workers_by_type['FitterActor']
        fitter_logic = FitterLogic()
        fitter_logic.set_conf(config['fitter_config'])
        fitter.tell({'command': 'SET_LOGIC', 'logic': fitter_logic})

    def configure_producer_supervisor(self, config):
        producers = self.producers  # TODO Fix outside
        producer_logics = [ProducerLogic(producer, 'output_topic') for producer in producers]
        producer_supervisor = self.workers_by_type['ProducerSupervisorActor']
        producer_actors_urns = list(producer_supervisor.ask({'command': 'STATUS'})['worker_statuses'].keys())
        producer_actors = [pykka.ActorRegistry.get_by_urn(urn) for urn in producer_actors_urns]
        for producer_actor, producer_logic in zip(producer_actors, producer_logics):
            producer_actor.tell({'command': 'SET_LOGIC', 'logic': producer_logic})

    # TODO: Implement this
    # def relink_workers(self, actor_class_name):
    #     if actor_class_name == 'InterpolatorActor':
    #         print("Relinking InterpolatorActor")
    #         self.link_datahandlers_to_interpolator()
    #         self.link_interpolator_to_fitter()
    #         self.link_interpolator_to_producer()

    def spawn_consumer_supervisor(self, num_workers):
        self.workers_by_type['ConsumerSupervisorActor'].ask({'command': 'SPAWN', 'config': {
            'actor_configs': [
                {} for _ in range(num_workers)
            ]
        }})

    def spawn_datahandler_supervisor(self, num_workers):
        self.workers_by_type['DataHandlerSupervisorActor'].ask({'command': 'SPAWN', 'config': {
            'actor_configs': [
                {} for _ in range(num_workers)
            ]
        }})

    def spawn_producer_supervisor(self):
        producer_supervisor = self.workers_by_type['ProducerSupervisorActor']
        producer_supervisor.ask({'command': 'SPAWN', 'config': {
            'actor_configs': [
                {}, {}, {},
            ]
        }})

    def spawn_base_system(self):
        self.actor_ref.tell({'command': 'SPAWN', 'config': {
            'CommanderActor': {
                'actor_configs': [
                    {}
                ]
            },

            'ConsumerSupervisorActor': {
                'actor_configs': [
                    {
                        'worker_class': ConsumerActor
                    }
                ]
            },

            'ProducerSupervisorActor': {
                'actor_configs': [
                    {
                        'worker_class': ProducerActor
                    }
                ]
            },

            'DataHandlerSupervisorActor': {
                'actor_configs': [
                    {
                        'worker_class': DataHandlerActor
                    }
                ]
            },

            'FitterActor': {
                'actor_configs': [
                    {}
                ]
            },

            'InterpolatorActor': {
                'actor_configs': [
                    {}
                ]
            }
        }})