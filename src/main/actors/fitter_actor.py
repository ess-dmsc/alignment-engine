import numpy as np
import pykka
from scipy.ndimage import convolve1d
from scipy.signal.windows import gaussian
from scipy.optimize import curve_fit


KNOWN_FIT_FUNCTIONS = ['gauss']


class FitterActor(pykka.ThreadingActor):
    def __init__(self, state_machine_supervisor, producer_actor=None, fitter_logic=None):
        super().__init__()
        self.state_machine_supervisor = state_machine_supervisor
        self.producer_actor = producer_actor
        self.fitter_logic = fitter_logic
        self.status = 'IDLE'

    def set_producer_actor(self, producer_actor):
        self.producer_actor = producer_actor

    def set_fitter_logic(self, fitter_logic):
        self.fitter_logic = fitter_logic

    def on_start(self):
        print(f"Starting {self.__class__.__name__}")
        self.state_machine_supervisor.tell({'command': 'REGISTER', 'actor': self.actor_ref})
        self.status = 'RUNNING'

    def on_failure(self, exception_type, exception_value, traceback):
        self.state_machine_supervisor.tell({'command': 'FAILED', 'actor': self.actor_ref})

    def on_receive(self, message):
        if not isinstance(message, dict):
            print(f"Unknown message: {message}")
            return

        command = message.get('command', None)

        if command is not None:
            if command == 'START':
                self.status = 'RUNNING'
                if self.fitter_logic:
                    self.fitter_logic.start()
                self.on_start()
            elif command == 'STOP':
                self.status = 'IDLE'
                if self.fitter_logic:
                    self.fitter_logic.stop()
            elif command == 'RESET':
                if self.fitter_logic:
                    self.fitter_logic.reset()
            elif command == 'STATUS':
                return self.get_status()
            elif command == 'SET_LOGIC':
                self.set_fitter_logic(message.get('logic', None))
            elif command == 'SET_PRODUCER_ACTOR':
                self.set_producer_actor(message.get('producer_actor', None))
            elif command == 'CONFIG':
                conf = message.get('config', None)
                if conf is not None and self.fitter_logic:
                    self.fitter_logic.set_conf(message['config'])
            return

        data = message.get('data', None)
        if data is not None:
            try:
                if self.fitter_logic:
                    self.fitter_logic.fit_data(message)
            except Exception as e:
                print(f"Fitter error: {e}")
            try:
                result = self.get_results()
                if result and self.producer_actor:
                    self.producer_actor.tell({'data': result})
            except Exception as e:
                print(f"Error getting and sending results from Fitter: {e}")
        else:
            print(f"Unknown message: {message}")

    def get_status(self):
        return self.status

    def get_results(self):
        if self.fitter_logic:
            return self.fitter_logic.get_results()

    def stop(self):
        pass


class FitterLogic:
    def __init__(self):
        self.active = True
        self.control_data = {}
        self.readout_data = {}
        self.control_signals = []
        self.readout_signals = []
        self.conf = {}
        self.fit_data_dict = {}

    def set_conf(self, conf):
        fit_function = conf.get('fit_function', None)
        if fit_function not in KNOWN_FIT_FUNCTIONS:
            raise ValueError(f"Unknown fit function: {fit_function}")
        self.conf = conf

    def fit_data(self, message):
        if not self.active:
            return

        data = message['data']

        self.control_signals = self.conf['control_signals']
        self.readout_signals = self.conf['readout_signals']

        for control_signal in self.control_signals:
            self.control_data[control_signal] = np.array(data[control_signal])
        for readout_signal in self.readout_signals:
            self.readout_data[readout_signal] = np.array(data[readout_signal])

        lengths = [len(x) for x in list(self.control_data.values()) + list(self.readout_data.values())]
        if len(set(lengths)) != 1:
            raise ValueError("Data have different lengths")
        if lengths[0] == 0:
            raise ValueError("Data are empty")

        if self.conf['fit_function'] == 'gauss':
            self._fit_gauss()

    def _fit_gauss(self):
        popts, r_squareds = fit_gaussian(
            *self.control_data.values(),
            *self.readout_data.values(),
        )
        for sender, popt, r_squared in zip(self.control_signals, popts, r_squareds):
            self.fit_data_dict[sender] = {
                'fit_params': popt,
                'r_squared': r_squared,
                'fit_function': 'gauss',
            }

    def get_results(self):
        return self.fit_data_dict

    def start(self):
        self.active = True

    def stop(self):
        self.active = False

    def reset(self):
        self.control_data = {}
        self.readout_data = {}
        self.control_signals = []
        self.readout_signals = []
        self.conf = {}
        self.fit_data_dict = {}


def gauss_func(x, h, a, x0, var):
    return h + a * np.exp(-(x - x0) ** 2 / (2 * var))


def fit_gaussian(x_vec, y_vec):
    # initial guesses for curve_fit
    mean = sum(x_vec * y_vec) / sum(y_vec)
    var = sum(y_vec * (x_vec - mean) ** 2) / sum(y_vec)
    if var < 1:
        var = 1.
    p0 = [min(y_vec), max(y_vec), mean, var]
    bounds = (
        [np.min(y_vec) - 1, 0., -np.inf, -np.inf],
        [np.max(y_vec) + 1, np.max(y_vec) + 1, np.inf, np.inf]
    )

    try:
        popt, pcov = curve_fit(
            gauss_func,
            x_vec,
            y_vec,
            p0=p0,
            bounds=bounds,
        )
    except ValueError as e:
        print(f"ValueError in curve_fit: {e}. Returning zeroes.")
        return [0., 0., 0., 0.], 0.
    except RuntimeError as e:
        print(f"RuntimeError in curve_fit: {e}. Returning zeroes.")
        return [0., 0., 0., 0.], 0.

    # Confidence interval (standard deviation)
    perr = np.sqrt(np.diag(pcov))

    # Evaluate fit
    residuals = y_vec - gauss_func(x_vec, *popt)
    ss_res = np.sum(residuals**2)
    ss_tot = np.sum((y_vec - np.mean(y_vec)) ** 2)
    r_squared = 1 - (ss_res / ss_tot)

    if r_squared < 0.5:
        # print("R-squared of fit is less than 0.5, returning zeroes.")
        return [0., 0., 0., 0.], 0.

    popt[3] = abs(popt[3])

    return [popt], [r_squared]


def _optimal_position_smooth(dev_vals, det_vals, gauss_factor=0.1):
    gauss_size = int(len(det_vals)*gauss_factor)
    window = gaussian(gauss_size, gauss_size/2)
    smooth_intensities = convolve1d(det_vals, window/window.sum())
    dev_peak = dev_vals[np.argmax(smooth_intensities)]
    return dev_peak
