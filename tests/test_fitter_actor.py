from unittest.mock import MagicMock

import pykka
import pytest
import numpy as np

from src.main.actors.fitter_actor import FitterLogic, FitterActor, gauss_func


def generate_fake_data_dict():
    event_t = np.linspace(0, 10, 201).astype(np.int64).tolist()
    data = {
        'sender1': event_t,
        'sender2': gauss_func(
                np.array(event_t),
                0.,
                100.,
                5.,
                1.,
            ).astype(np.int64).tolist(),
    }
    return {'data': data}


def generate_fake_data_dict_with_missing_key():
    event_t = np.linspace(0, 10, 201).astype(np.int64).tolist()
    data = {
        'sender1': event_t,
        # Missing sender2 here
    }
    return {'data': data}




class TestFitterActor:
    @pytest.fixture
    def fitter_actor_setup(self):
        self.state_machine_actor_mock = MagicMock()
        self.producer_actor_mock = MagicMock()
        self.fitter_logic_mock = MagicMock(spec=FitterLogic)
        self.actor_ref = FitterActor.start(
            self.state_machine_actor_mock,
            self.producer_actor_mock,
            self.fitter_logic_mock)
        self.actor_proxy = self.actor_ref.proxy()
        yield
        pykka.ActorRegistry.stop_all()

    def test_on_start_fitter_actor(self, fitter_actor_setup):
        self.actor_proxy.on_receive('START').get()
        assert self.actor_proxy.get_status().get() == 'RUNNING'

    def test_on_receive_stop_fitter_actor(self, fitter_actor_setup):
        self.actor_proxy.on_receive('STOP').get()
        assert self.actor_proxy.get_status().get() == 'IDLE'

    def test_on_receive_reset_fitter_actor(self, fitter_actor_setup):
        self.actor_proxy.on_receive('RESET').get()
        self.fitter_logic_mock.reset.assert_called_once()

    def test_on_receive_data_fitter_actor(self, fitter_actor_setup):
        fake_data = generate_fake_data_dict()
        self.actor_proxy.on_receive(fake_data).get()
        self.fitter_logic_mock.fit_data.assert_called_once_with(fake_data)

    def test_get_results_interpolator_actor(self, fitter_actor_setup):
        self.actor_proxy.get_results().get()
        self.fitter_logic_mock.get_results.assert_called_once()

    def test_outgoing_data_to_fitting_actor(self, fitter_actor_setup):
        fake_data = generate_fake_data_dict()
        expected_result = {
            'sender1': {
                'fit_params': [0, 100, 5, 1],
                'r_squared': '1',
            }
        }
        self.fitter_logic_mock.get_results.return_value = expected_result
        self.actor_proxy.on_receive(fake_data).get()

        self.producer_actor_mock.tell.assert_called_once_with({'data': expected_result})



@pytest.fixture
def fitter():
    return FitterLogic()


def test_on_data_received(fitter):
    fake_data = generate_fake_data_dict()
    conf = {
        'control_signals': ['sender1'],
        'readout_signals': ['sender2'],
        'fit_function': 'gauss'
    }
    fitter.set_conf(conf)
    fitter.fit_data(fake_data)

    assert np.allclose(fitter.fit_data_dict['sender1']['fit_params'], [0, 100, 5, 1], atol=0.1)


@pytest.mark.parametrize("conf, data, expected_error", [
    # test case when one of the required keys is missing in data
    ({
         'control_signals': ['sender1'],
         'readout_signals': ['sender2'],
         'fit_function': 'gauss'
     }, generate_fake_data_dict_with_missing_key(), KeyError),

    # test case when fit function is not supported
    ({
         'control_signals': ['sender1'],
         'readout_signals': ['sender2'],
         'fit_function': 'invalid_function'
     }, generate_fake_data_dict(), ValueError),
])
def test_fit_data_exceptions(fitter, conf, data, expected_error):
    with pytest.raises(expected_error):
        fitter.set_conf(conf)
        fitter.fit_data(data)


@pytest.mark.parametrize("data", [
    # test case when control_data and readout_data are empty
    ({
         'sender1': [],
         'sender2': [],
     }),

    # test case when control_data and readout_data have different lengths
    ({
         'sender1': [1, 2, 3, 4, 5],
         'sender2': [1, 2, 3],
     }),
])
def test_fit_data_weird_values(fitter, data):
    conf = {
        'control_signals': ['sender1'],
        'readout_signals': ['sender2'],
        'fit_function': 'gauss'
    }
    fitter.set_conf(conf)
    with pytest.raises(ValueError):
        fitter.fit_data({'data': data})
