import multiprocessing
import os
import time
from queue import Full

import numpy as np
import pytest
from src.main.actors.interpolator_actor import InterpolatorLogic


# Fixture for InterpolatorLogic setup
@pytest.fixture
def interpolator_logic():
    return InterpolatorLogic()


# Function to generate data for testing
def generate_data(sender, value, timestamp):
    return {'sender': sender, 'data': {'value': value, 'time': timestamp}}


def test_process_data_method(interpolator_logic):
    sender1 = 'sender1'
    sender2 = 'sender2'
    data1 = generate_data(sender1, [3, 4, 5], [1, 2, 3])
    data2 = generate_data(sender2, [4, 5, 6], [1, 2, 3])

    try:
        interpolator_logic.process_data(data1)
        interpolator_logic.process_data(data2)
    except Exception as e:
        pytest.fail(f"process_data raised an unexpected exception: {e}")

    assert sender1 in interpolator_logic.raw_data
    assert sender2 in interpolator_logic.raw_data
    assert interpolator_logic.raw_data[sender1]['value'] == data1['data']['value']
    assert interpolator_logic.raw_data[sender1]['time'] == data1['data']['time']
    assert interpolator_logic.raw_data[sender2]['value'] == data2['data']['value']
    assert interpolator_logic.raw_data[sender2]['time'] == data2['data']['time']


def test_interpolate_to_common_timestamps_method(interpolator_logic):
    sender1 = 'sender1'
    sender2 = 'sender2'
    data1 = generate_data(sender1, [1, 2, 3], [1, 2, 3])
    data2 = generate_data(sender2, [4, 5, 6], [2, 3, 4])

    interpolator_logic.process_data(data1)
    interpolator_logic.process_data(data2)

    common_ts, _, interp_data = interpolator_logic.interpolate_to_common_timestamps(
        *interpolator_logic.get_ordered_raw_data()
    )

    assert len(common_ts) == 4
    assert np.allclose(interp_data[0], [1.0, 2.0, 3.0, 4.0])
    assert np.allclose(interp_data[1], [3.0, 4.0, 5.0, 6.0])


# Test case for invalid data in process_data
def test_process_data_invalid(interpolator_logic):
    # Test data with missing keys
    data_missing_keys = {'sender': 'sender1', 'data': {}}
    with pytest.raises(KeyError):
        interpolator_logic.process_data(data_missing_keys)

    # Test data with wrong types
    data_wrong_types = generate_data('sender1', 'not a list', 'not a list')
    with pytest.raises(ValueError):  # Adjust this depending on what exception you expect
        interpolator_logic.process_data(data_wrong_types)


# Test case for invalid data in interpolate_to_common_timestamps
def test_interpolate_to_common_timestamps_invalid(interpolator_logic):
    sender1 = 'sender1'
    sender2 = 'sender2'
    data1 = generate_data(sender1, [1, 2, 3], [1, 2])  # Timestamps and data are different lengths
    data2 = generate_data(sender2, [4, 5, 6], [2, 3, 4])

    interpolator_logic.process_data(data1)
    with pytest.raises(AssertionError):
        interpolator_logic.process_data(data2)


def test_getting_results_to_main_using_queue(interpolator_logic):
    sender1 = 'sender1'
    sender2 = 'sender2'
    data1 = generate_data(sender1, [1, 2, 3], [1, 2, 3])
    data2 = generate_data(sender2, [4, 5, 6], [2, 3, 4])

    for data in [data1, data2]:
        interpolator_logic.process_data(data)

    results = interpolator_logic.get_results()

    assert len(results) != 0, "Result should not be empty"


def test_main_process_is_not_blocked(interpolator_logic):
    data = generate_data('sender', [1, 2, 3], [1, 2, 3])
    interpolator_logic.process_data(data)

    # If we get to this point, the main process was not blocked
    assert True


def test_exceptions_are_handled(interpolator_logic):
    message = 'not a valid message'
    with pytest.raises(ValueError):
        interpolator_logic.process_data(message)


def test_interpolate_to_common_timestamps(interpolator_logic):
    sender1 = 'sender1'
    sender2 = 'sender2'
    data1 = generate_data(sender1, [1, 2, 3], [1, 2, 3])
    data2 = generate_data(sender2, [4, 5, 6], [2, 3, 4])

    for data in [data1, data2]:
        interpolator_logic.process_data(data)

    results = interpolator_logic.get_results()
    time1 = results[-1]["sender1"]["time"]
    time2 = results[-1]["sender2"]["time"]
    value1 = results[-1]["sender1"]["value"]
    value2 = results[-1]["sender2"]["value"]

    assert np.all(time1 == time2)
    assert len(time1) == 4
    assert np.allclose(value1, [1.0, 2.0, 3.0, 4.0])
    assert np.allclose(value2, [3.0, 4.0, 5.0, 6.0])


def test_multiple_messages_different_senders(interpolator_logic):
    num_messages = 100
    value_list = [[i + 1, i + 2, i + 3, i + 4] for i in range(num_messages)]
    time_list = [[i + 1, i + 2, i + 3, i + 4] for i in range(num_messages)]

    data_list = [
        generate_data(f'sender{i}', v, t)
        for i, (v, t) in enumerate(zip(value_list, time_list))
    ]

    for data in data_list:
        interpolator_logic.process_data(data)

    results = interpolator_logic.get_results()

    assert len(results) == num_messages - 1, f"Should receive {num_messages - 1} results"
    for i, result in enumerate(results):
        i += 1
        sender = f'sender{i}'
        expected_time = np.unique(np.concatenate(time_list[:i+1]))
        expected_value = np.unique(np.concatenate(value_list[:i+1]))
        assert np.allclose(result[sender]['time'], expected_time), f"Time for {sender} incorrect"
        assert np.allclose(result[sender]['value'], expected_value), f"Values for {sender} incorrect"
        assert sender in result, f"Result for {sender} not in results"


def test_multiple_messages_same_sender(interpolator_logic):
    num_messages = 200
    value_list = [np.arange(1, 500, 1).tolist() for i in range(num_messages)]
    time_list = [np.arange(1, 500, 1).tolist() for i in range(num_messages)]
    sender_list = [0, 1] * (num_messages // 2)

    data_list = [
        generate_data(f'sender{i}', v, t)
        for i, v, t in zip(sender_list, value_list, time_list)
    ]

    for data in data_list:
        interpolator_logic.process_data(data)

    results = interpolator_logic.get_results()

    assert len(results) == num_messages - 1, f"Should receive {num_messages - 1} results"
    for i, (sender_id, result) in enumerate(zip(sender_list[1::], results)):
        i += 1
        sender = f'sender{sender_id}'
        expected_time = np.unique(np.concatenate(time_list[i-1:i + 1]))
        expected_value = np.unique(np.concatenate(value_list[i-1:i + 1]))
        assert np.allclose(result[sender]['time'], expected_time), f"Time for {sender} incorrect"
        assert np.allclose(result[sender]['value'], expected_value), f"Values for {sender} incorrect"
        assert sender in result, f"Result for {sender} not in results"






