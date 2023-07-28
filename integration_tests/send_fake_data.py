import time

from confluent_kafka import Producer

from social_tests.test_actor_data_pipeline import generate_gauss_f144_data, generate_gauss_ev44_events

producer = Producer({"bootstrap.servers": 'localhost:9092', "message.max.bytes": 100_000_000})

f144_data = generate_gauss_f144_data(201)
ev44_data = generate_gauss_ev44_events(401)


idx_1 = 0
idx_2 = 0
while True:
    if idx_1 < 200:
        producer.produce("motor_data", f144_data[idx_1])
        idx_1 += 1
        producer.flush()

    if idx_2 < 400:
        producer.produce("event_data", ev44_data[idx_2])
        idx_2 += 1
        producer.flush()

    time.sleep(0.1)

    if idx_2 < 400:
        producer.produce("event_data", ev44_data[idx_2])
        idx_2 += 1
        producer.flush()

    else:
        break

    time.sleep(0.1)

