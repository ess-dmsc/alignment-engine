from tests.doubles.consumer import ConsumerStub
from tests.doubles.producer import ProducerSpy

TEST_CONSUMERS = [ConsumerStub({}), ConsumerStub({})]
TEST_PRODUCERS = [ProducerSpy({}), ProducerSpy({}), ProducerSpy({})]