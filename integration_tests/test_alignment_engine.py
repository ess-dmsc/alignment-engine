import json
import os
import sys
import threading
import time

import pytest
from confluent_kafka import Consumer
from streaming_data_types import deserialise_f144

from integration_tests.send_fake_data import generate_and_produce_data
from integration_tests.send_fake_start import produce_start, produce_stop
from integration_tests.send_fake_config import produce_config

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))


class TestAlignmentEngine:
    @pytest.fixture(autouse=True)
    def prepare(self):
        time.sleep(1)
        print("produce_config")
        produce_config()
        time.sleep(1)
        print("produce_start")
        produce_start()
        time.sleep(1)
        # print("generate_and_produce_data")
        # generate_and_produce_data()
        data_thread = threading.Thread(target=generate_and_produce_data)
        data_thread.start()
        time.sleep(0.1)
        yield
        data_thread.join()

    def get_data_from_kafka(self):
        consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',  # replace with your server address
            'group.id': 'my_group',
            'auto.offset.reset': 'earliest'
        })

        consumer.subscribe(['output_topic'])  # replace with your topic name

        counter = 0
        try:
            while True:
                # print(f"counter: {counter}")
                counter += 1
                if counter > 1000:
                    break
                msg = consumer.poll(0.05)
                if msg is None:
                    continue
                data = deserialise_f144(msg.value())
                print(f"Received data: {data}")
                return data
        except KeyboardInterrupt:
            print('Stopped by user')
        finally:
            consumer.close()

        return None

    def test_basic_operation(
        self, alignment_engine
    ):
        print("test_basic_operation")

        data = self.get_data_from_kafka()
        assert data is not None
        # # Configure just-bin-it
        # config = self.create_basic_config()
        # self.send_message(CMD_TOPIC, bytes(json.dumps(config), "utf-8"))
        #
        # # Give it time to start counting
        # time.sleep(1)
        #
        # # Send fake data
        # num_msgs = 10
        #
        # for i in range(num_msgs):
        #     self.generate_and_send_data(i + 1)
        #     time.sleep(0.5)
        #
        # total_events = sum(self.num_events_per_msg)
        #
        # time.sleep(10)
        #
        # # Get histogram data
        # hist_data = self.get_hist_data_from_kafka()
        #
        # assert hist_data["data"].sum() == total_events
        # assert json.loads(hist_data["info"])["state"] == "COUNTING"
        #
        # self.send_message(CMD_TOPIC, bytes(json.dumps(STOP_CMD), "utf-8"))
        # time.sleep(1)
        #
        # # Get histogram data
        # hist_data = self.get_hist_data_from_kafka()
        #
        # assert hist_data["data"].sum() == total_events
        # assert json.loads(hist_data["info"])["state"] == "FINISHED"

