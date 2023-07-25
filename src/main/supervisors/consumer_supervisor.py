import time

import pykka
from confluent_kafka import Consumer
from pykka import ActorRegistry

from src.main.actors.consumer_actor import ConsumerLogic
from src.main.actors.data_handler_actor import DataHandlerActor
from src.main.actors.producer_actor import ProducerActor
from src.main.supervisors.base_supervisor import BaseSupervisorActor
from tests.doubles.consumer import ConsumerStub


class ConsumerSupervisorActor(BaseSupervisorActor):
    def __init__(self, supervisor, worker_class):
        super().__init__(supervisor, worker_class)
    #     self.config = None
    #
    # def on_receive(self, message):
    #     super().on_receive(message)
    #
    #     command = message.get('command', None)
    #     if command is not None:
    #         if command == 'CONFIG':
    #             self.config = message.get('config', None)
    #             self.update_workers()
    #
    # def build_spawn_config(self):
    #     if self.config is None:
    #         return None
    #
    #     # datahandler_supervisor_actor = self.supervisor.proxy().workers_by_type.get()['DataHandlerSupervisorActor']
    #     #
    #     # status_dict = datahandler_supervisor_actor.ask({'command': 'STATUS'})
    #     # print(f"STATUS DICT: {status_dict}")
    #     # print(f"STATUS DICT TYPE: {type(status_dict)}")
    #     #
    #     # try:
    #     #     print(f"datahandler supervisor is alive: {list(datahandler_supervisor_actor.ask({'command': 'STATUS'})['worker_statuses'].keys())}")
    #     # except Exception as e:
    #     #     print("failed to read status: ", e)
    #
    #     # datahandler_actors_urns = list(datahandler_supervisor_actor.ask({'command': 'STATUS'})['worker_statuses'].keys())
    #     # datahandler_actors = [pykka.ActorRegistry.get_by_urn(urn) for urn in datahandler_actors_urns]
    #
    #     producer_actors = ActorRegistry.get_by_class(ProducerActor)
    #     print(f"producer actors: {producer_actors}")
    #
    #     datahandler_actors = ActorRegistry.get_by_class(DataHandlerActor)
    #     print(f"datahandler actors: {datahandler_actors}")
    #
    #     spawn_config = {'command': 'SPAWN', 'config': {
    #         'actor_configs': []
    #     }}
    #
    #     for i, stream in enumerate(self.config):
    #         # consumer = Consumer({
    #         #     'bootstrap.servers': 'localhost:9092',
    #         #     'group.id': 'mygroup',
    #         #     'auto.offset.reset': 'latest'
    #         # })
    #         # consumer.subscribe(['mytopic'])
    #         consumer = ConsumerStub({})
    #
    #         consumer_logic = ConsumerLogic(consumer, source_name=stream['source'])
    #
    #         spawn_config['config']['actor_configs'].append({
    #             'data_handler_actor': datahandler_actors[i],
    #             'consumer_logic': consumer_logic
    #         })
    #
    #     return spawn_config
    #
    # def update_workers(self):
    #     # for worker in self.workers.values():
    #     #     worker.tell({'command': 'KILL'})
    #
    #     spawn_config = self.build_spawn_config()
    #     if spawn_config is not None:
    #         for actor_config in spawn_config['config']['actor_configs']:
    #             actor = self.worker_class.start(self.actor_ref, **actor_config)
    #             self.workers[actor.actor_urn] = actor
    #             self.workers_configs[actor.actor_urn] = actor_config
