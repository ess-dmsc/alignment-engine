import pytest
import pykka

from src.main.supervisors.consumer_supervisor import ConsumerSupervisorActor
from src.main.supervisors.producer_supervisor import ProducerSupervisorActor
from src.main.supervisors.state_machine_supervisor import StateMachineSupervisorActor
from src.main.supervisors.data_handler_supervisor import DataHandlerSupervisorActor
from src.main.actors.interpolator_actor import InterpolatorActor
from src.main.actors.fitter_actor import FitterActor
from src.main.actors.commander_actor import CommanderActor



class TestStateMachineSupervisorActor:
    @pytest.fixture
    def supervisor(self):
        supervisor = StateMachineSupervisorActor.start(None, [
            ConsumerSupervisorActor,
            ProducerSupervisorActor,
            DataHandlerSupervisorActor,
            InterpolatorActor,
            FitterActor,
            CommanderActor,
        ])

        yield supervisor
        pykka.ActorRegistry.stop_all()