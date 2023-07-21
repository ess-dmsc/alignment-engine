from src.main.supervisors.base_supervisor import BaseSupervisorActor


class ConsumerSupervisorActor(BaseSupervisorActor):
    def __init__(self, worker_class, supervisor, **kwargs):
        super().__init__(worker_class, **kwargs)
        self.supervisor = supervisor
