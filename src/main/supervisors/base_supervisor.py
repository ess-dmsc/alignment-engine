import time

import pykka


class BaseSupervisorActor(pykka.ThreadingActor):
    def __init__(self, worker_class, supervisor=None, **kwargs):
        super().__init__()
        self.worker_class = worker_class
        self.supervisor = supervisor
        self.workers = {}
        self.workers_configs = {}

    def on_start(self):
        print(f"Starting {self.__class__.__name__}")
        if self.supervisor is None:
            return
        self.supervisor.tell({'command': 'REGISTER', 'actor': self.actor_ref})

    def on_failure(self, exception_type, exception_value, traceback):
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
            if actor.actor_urn not in self.workers:
                print(f"{actor.__class__.__name__} {actor.actor_urn} has died. Not this supervisor's worker")
                return
            print(f"{actor.__class__.__name__} {actor.actor_urn} has died. Restarting...")
            actor_config = self.workers_configs[actor.actor_urn]
            new_actor = self.worker_class.start(self.actor_ref, **actor_config)
            print(f"New {new_actor.__class__.__name__} {new_actor.actor_urn} has been started")
            del self.workers[actor.actor_urn]
            self.workers[new_actor.actor_urn] = new_actor

        elif command == 'SPAWN':
            config = message.get('config', None)
            if config is None:
                raise ValueError("No config provided for SPAWN command")
            actor_configs = config['actor_configs']
            for actor_config in actor_configs:
                actor = self.worker_class.start(self.actor_ref, **actor_config)
                self.workers[actor.actor_urn] = actor
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

