import pykka


class TestWorkerActor(pykka.ThreadingActor):
    __test__ = False

    def __init__(self, supervisor):
        super().__init__()
        self.supervisor = supervisor

    def on_start(self):
        print(f"Starting {self.__class__.__name__}")
        self.supervisor.tell({'command': 'REGISTER', 'actor': self.actor_ref})

    def on_failure(self, exception_type, exception_value, traceback):
        self.supervisor.tell({'command': 'FAILED', 'actor': self.actor_ref})

    def on_receive(self, message):
        command = message.get('command')

        if command == 'STATUS':
            return "Test worker actor is alive"

        if command == 'FAIL':
            print(f"Test worker actor {self.actor_urn} is failing")
            raise Exception("Test exception")


class TestWorkerActor2(TestWorkerActor):
    __test__ = False

    def __init__(self, supervisor, some_other_actor, some_logic_class, **kwargs):
        super().__init__(supervisor)
        self.some_other_actor = some_other_actor
        self.some_logic_class = some_logic_class