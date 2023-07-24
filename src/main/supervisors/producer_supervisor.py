from src.main.supervisors.base_supervisor import BaseSupervisorActor


class ProducerSupervisorActor(BaseSupervisorActor):
    def __init__(self, supervisor, worker_class):
        super().__init__(supervisor, worker_class)