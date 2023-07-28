import time

import numpy as np
import pykka
from scipy.interpolate import interp1d


class InterpolatorActor(pykka.ThreadingActor):
    def __init__(self, state_machine_supervisor, fitter_actor=None, producer_actor=None, interpolator_logic=None):
        super().__init__()
        self.state_machine_supervisor = state_machine_supervisor
        self.fitter_actor = fitter_actor
        self.producer_actor = producer_actor
        self.interpolator_logic = interpolator_logic
        self.status = 'IDLE'

    def set_fitter_actor(self, fitter_actor):
        self.fitter_actor = fitter_actor

    def set_producer_actor(self, producer_actor):
        self.producer_actor = producer_actor

    def set_interpolator_logic(self, interpolator_logic):
        self.interpolator_logic = interpolator_logic

    def get_config(self):
        config = {
            'interpolator_logic': self.interpolator_logic,
            'fitter_actor': self.fitter_actor,
            'producer_actor': self.producer_actor,
        }
        return config

    def on_start(self):
        print(f"Starting {self.__class__.__name__}")
        self.state_machine_supervisor.tell({'command': 'REGISTER', 'actor': self.actor_ref})
        if self.interpolator_logic is not None:
            self.interpolator_logic.start()
        self.status = 'RUNNING'

    def on_failure(self, exception_type, exception_value, traceback):
        self.state_machine_supervisor.tell({'command': 'FAILED', 'actor': self.actor_ref, 'actor_class_name': self.__class__.__name__, 'last_config': self.get_config()})

    def on_receive(self, message):
        if not isinstance(message, dict):
            print(f"Unknown message: {message}")
            return

        command = message.get('command', None)

        if command is not None:
            if command == 'START':
                if self.interpolator_logic is not None:
                    self.interpolator_logic.start()
                self.on_start()
            elif command == 'STOP':
                if self.interpolator_logic is not None:
                    self.interpolator_logic.stop()
                self.status = 'IDLE'
            elif command == 'RESET':
                if self.interpolator_logic is not None:
                    self.interpolator_logic.reset()
            elif command == 'STATUS':
                return self.get_status()
            elif command == 'SET_LOGIC':
                self.set_interpolator_logic(message.get('logic', None))
            elif command == 'SET_PRODUCER_ACTOR':
                self.set_producer_actor(message.get('producer_actor', None))
            elif command == 'SET_FITTER_ACTOR':
                self.set_fitter_actor(message.get('fitter_actor', None))
            return

        data = message.get('data', None)
        if data is not None:
            try:
                if self.interpolator_logic is not None:
                    self.interpolator_logic.process_data(message)
            except Exception as e:
                print(f"Interpolator error: {e}")
            try:
                result = self.get_results()
                if result:
                    if self.fitter_actor is not None:
                        self.fitter_actor.tell({'data': result})
                    if self.producer_actor is not None and self.producer_actor.is_alive():
                        self.producer_actor.tell({'data': result})
            except Exception as e:
                print(f"Error getting and sending results from Interpolator: {e}")
        else:
            print(f"Unknown message: {message}")

    def get_status(self):
        return self.status

    def get_results(self):
        if self.interpolator_logic is not None:
            return self.interpolator_logic.get_results()

    def stop(self):
        if self.interpolator_logic is not None:
            self.interpolator_logic.stop()


class InterpolatorLogic:
    def __init__(self):
        self.active = True
        self.raw_data = {}
        self.interpolated_data = {}

    def process_data(self, message):
        if not self.active:
            return

        if 'sender' not in message:
            raise ValueError("Message must have a sender")

        sender = message['sender']

        if sender is None:
            return

        if sender not in self.raw_data:
            self.raw_data[sender] = {'value': [], 'time': []}

        if type(message['data']['value']) != list:
            raise ValueError("Value must be a list")
        elif type(message['data']['time']) != list:
            raise ValueError("Time must be a list")

        self.raw_data[sender]['value'] = message['data']['value']
        self.raw_data[sender]['time'] = message['data']['time']

        if len(self.raw_data.keys()) < 2:
            return

        common_ts, _, interp_data = self.interpolate_to_common_timestamps(
            *self.get_ordered_raw_data()
        )

        self.interpolated_data = {sender: data for sender, data in
                                  zip(self.raw_data.keys(), interp_data)}

        return self.interpolated_data

    def get_ordered_raw_data(self):
        return [val for sender in self.raw_data.keys() for key in ['time', 'value'] for val in [self.raw_data[sender][key]]]

    def interpolate_to_common_timestamps(self, *args):
        ts_data_pairs = list(zip(*[iter(args)] * 2))

        for pair in ts_data_pairs:
            assert len(pair[0]) == len(pair[1]), "Data and timestamps must be the same length"
            assert len(pair[0]) == len(np.unique(pair[0])), "Timestamps must be unique"
            assert len(pair[0]) > 1, "Must have more than one timestamp"
            assert np.min(pair[0]) >= 0, "Timestamps must be positive"

        common_ts = np.unique(np.concatenate([ts for ts, _ in ts_data_pairs]))

        assert len(common_ts) > 1, "Must have more than one timestamp"

        interp_funcs = []
        interp_data = []
        for ts, data in ts_data_pairs:
            # fill_value = data[-1]
            fill_value = 'extrapolate'
            interp = interp1d(ts, data, kind='linear', fill_value=fill_value, bounds_error=False)
            interp_funcs.append(interp)
            interp_data.append(interp(common_ts))

        return common_ts, interp_funcs, interp_data

    def start(self):
        self.active = True

    def stop(self):
        self.active = False

    def reset(self):
        self.interpolated_data.clear()
        self.raw_data.clear()
        self.active = True

    def get_results(self):
        return self.interpolated_data
