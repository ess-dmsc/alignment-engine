import numpy as np
import pykka


class DataHandlerActor(pykka.ThreadingActor):
    def __init__(self, data_handler_supervisor, interpolator_actor=None, data_handler_logic=None):
        super().__init__()
        self.data_handler_supervisor = data_handler_supervisor
        self.interpolator_actor = interpolator_actor
        self.data_handler_logic = data_handler_logic
        if self.data_handler_logic is not None:
            self.data_handler_logic.active = True
        self.status = 'IDLE'

    def set_interpolator_actor(self, interpolator_actor):
        self.interpolator_actor = interpolator_actor

    def set_data_handler_logic(self, data_handler_logic):
        self.data_handler_logic = data_handler_logic
        if self.data_handler_logic is not None:
            self.data_handler_logic.active = True

    def on_start(self):
        print(f"Starting {self.__class__.__name__}")
        self.data_handler_supervisor.tell({'command': 'REGISTER', 'actor': self.actor_ref})
        if self.data_handler_logic is not None:
            self.data_handler_logic.start()
        self.status = 'RUNNING'

    def on_failure(self, exception_type, exception_value, traceback):
        self.data_handler_supervisor.tell({'command': 'FAILED', 'actor': self.actor_ref})

    def on_receive(self, message):
        if not isinstance(message, dict):
            print(f"Unknown message: {message}")
            return

        command = message.get('command', None)

        if command is not None:
            if command == 'START':
                if self.data_handler_logic is not None:
                    self.data_handler_logic.start()
            elif command == 'STOP':
                self.stop()
            elif command == 'RESET':
                if self.data_handler_logic is not None:
                    self.data_handler_logic.reset()
            elif command == 'STATUS':
                return self.get_status()
            elif command == 'SET_LOGIC':
                self.set_data_handler_logic(message.get('logic', None))
            elif command == 'SET_INTERPOLATOR_ACTOR':
                self.set_interpolator_actor(message.get('interpolator_actor', None))
            return

        data = message.get('data', None)
        if data is not None:
            try:
                if self.data_handler_logic is not None:
                    self.data_handler_logic.on_data_received(message)
                    self.send_to_interpolator()
            except Exception as e:
                print(f"Data handler error: {e}")
        else:
            print(f"Unknown message: {message}")

    def get_status(self):
        return self.status

    def stop(self):
        if self.data_handler_logic is not None:
            self.data_handler_logic.stop()
        super().stop()

    def send_to_interpolator(self):
        if self.interpolator_actor is not None:
            data = {
                'sender': self.data_handler_logic.source_name,  # Assuming actor_urn as sender's unique identifier
                'data': {
                    'value': self.data_handler_logic.value_data,
                    'time': self.data_handler_logic.time_data,
                }
            }
            self.interpolator_actor.tell(data)


class DataHandlerLogic:
    def __init__(self):
        self.active = True
        self.value_data = []
        self.time_data = []
        self.source_name = None

    def on_data_received(self, message):
        if not self.active:
            return

        data = message['data']
        data_type = self.get_data_type(data)
        if data_type is None:
            return

        value_data, time_data = self.process_data(data, data_type)
        if None not in (value_data, time_data, self.source_name):
            self.value_data.append(value_data)
            self.time_data.append(time_data)

    def process_data(self, data, data_type):
        value_data = time_data = None

        if data_type == 'ev44':
            value_data, time_data = self.process_ev44_data(data)
        elif data_type == 'f144':
            value_data, time_data = self.process_f144_data(data)

        return value_data, time_data

    def process_ev44_data(self, data):
        try:
            value_data = len(data.time_of_flight)
            if value_data == 0:
                return 0, data.reference_time[0]
            time_data = data.reference_time[0] + np.min(data.time_of_flight)
            self.source_name = data.source_name
        except TypeError:
            return None, None

        return value_data, time_data

    def process_f144_data(self, data):
        try:
            value_data = data.value
            time_data = data.timestamp_unix_ns
            self.source_name = data.source_name
        except TypeError:
            return None, None

        return value_data, time_data

    def get_data_type(self, data):
        if 'ev44' in str(type(data)):
            return 'ev44'
        elif 'f144' in str(type(data)):
            return 'f144'
        else:
            return None

    def start(self):
        self.active = True

    def stop(self):
        self.active = False

    def reset(self):
        self.value_data.clear()
        self.time_data.clear()
        self.active = True
