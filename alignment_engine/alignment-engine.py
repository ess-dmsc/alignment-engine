import time
import pykka

# import os
# import sys
#
# cwd = os.getcwd()
# sys.path.append(cwd)

from main.actors.commander_actor import CommanderActor, CommanderLogic
from main.actors.fitter_actor import FitterActor
from main.actors.interpolator_actor import InterpolatorActor
from main.kafka_factories.consumer import ConsumerFactory
from main.kafka_factories.producer import ProducerFactory
from main.supervisors.consumer_supervisor import ConsumerSupervisorActor
from main.supervisors.data_handler_supervisor import DataHandlerSupervisorActor
from main.supervisors.producer_supervisor import ProducerSupervisorActor
from main.supervisors.state_machine_supervisor import StateMachineSupervisorActor


def main():
    try:
        consumer_factory = ConsumerFactory()
        producer_factory = ProducerFactory()
        worker_classes = [
            ConsumerSupervisorActor,
            ProducerSupervisorActor,
            DataHandlerSupervisorActor,
            InterpolatorActor,
            FitterActor,
            CommanderActor,
        ]
        state_machine_supervisor = StateMachineSupervisorActor.start(None, worker_classes, consumer_factory, producer_factory)

        time.sleep(0.5)

        commander_consumer = consumer_factory.create_consumer("localhost:9092", "alien_commands")
        commander_logic = CommanderLogic(commander_consumer)
        commander_actor = state_machine_supervisor.proxy().workers_by_type.get()['CommanderActor']
        commander_actor.ask({'command': 'SET_LOGIC', 'logic': commander_logic})

        time.sleep(0.5)

        commander_actor.ask({'command': 'START'})
        while True:  # Infinite loop
            time.sleep(0.5)  # you may adjust this time or remove it entirely

    except KeyboardInterrupt:
        print("\nInterrupted by user")
    finally:
        print("Stopping all actors and performing cleanup...")
        pykka.ActorRegistry.stop_all()


if __name__ == '__main__':
    main()


