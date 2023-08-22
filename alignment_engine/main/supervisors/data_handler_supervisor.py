from main.supervisors.base_supervisor import BaseSupervisorActor


class DataHandlerSupervisorActor(BaseSupervisorActor):
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
    #     interpolator_actor = self.supervisor.proxy().workers_by_type.get()['InterpolatorActor']
    #
    #     spawn_config = {'command': 'SPAWN', 'config': {
    #         'actor_configs': []
    #     }}
    #
    #     for i in range(len(self.config['stream_configs'])):
    #         datahandler_logic = DataHandlerLogic()
    #
    #         spawn_config['config']['actor_configs'].append({
    #             'interpolator_actor': interpolator_actor,
    #             'data_handler_logic': datahandler_logic
    #         })
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
    #             time.sleep(0.05)
    #             actor.tell({'command': 'START'})
