import numpy as np
import pykka
from scipy.interpolate import interp1d


class InterpolatorActor(pykka.ThreadingActor):
    def __init__(self, state_machine_actor, fitting_actor, producer_actor, interpolator_logic):
        super().__init__()
        self.state_machine_actor = state_machine_actor
        self.fitting_actor = fitting_actor
        self.producer_actor = producer_actor
        self.interpolator_logic = interpolator_logic
        self.status = 'IDLE'

    def on_receive(self, message):
        if message == 'START':
            self.status = 'RUNNING'
            self.interpolator_logic.start()
            self.on_start()
        elif message == 'STOP':
            self.status = 'IDLE'
            self.interpolator_logic.stop()
        elif message == 'RESET':
            self.interpolator_logic.reset()
        elif isinstance(message, dict) and 'data' in message:
            try:
                self.interpolator_logic.process_data(message)
            except Exception as e:
                print(f"Interpolator error: {e}")
            try:
                result = self.get_results()
                if result:
                    self.fitting_actor.tell({'data': result})
                    self.producer_actor.tell({'data': result})
            except Exception as e:
                print(f"Error getting and sending results from Interpolator: {e}")
        else:
            print(f"Unknown message: {message}")

    def on_start(self):
        pass

    def get_status(self):
        return self.status

    def get_results(self):
        return self.interpolator_logic.get_results()

    def stop(self):
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
            interp = interp1d(ts, data, kind='linear', fill_value='extrapolate')
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
