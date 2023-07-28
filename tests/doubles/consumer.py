import time


class Message:
    def __init__(self, topic, partition, offset, key, value, error=None):
        self.topic = topic
        self.partition = partition
        self._offset = offset
        self.key = key
        self._value = value
        self._error = error

    def value(self):
        return self._value

    def offset(self):
        return self._offset

    def error(self):
        return self._error


class ExceptionMessage(Message):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def value(self):
        raise Exception("Value method called on ExceptionMessage")

    def offset(self):
        raise Exception("Offset method called on ExceptionMessage")

    def error(self):
        raise Exception("Error method called on ExceptionMessage")


class ConsumerStub:
    def __init__(self, config):
        self.config = config
        self.messages = []
        self.current_offset = 0

    def poll(self, timeout=None):
        if self.current_offset >= len(self.messages):
            return None  # No more messages

        msg = self.messages[self.current_offset]
        self.current_offset += 1
        return msg

    def add_message(self, topic, partition, offset, key, value, error=None):
        self.messages.append(Message(topic, partition, offset, key, value, error))

    def add_exception_message(self, topic, partition, offset, key, value, error=None):
        self.messages.append(ExceptionMessage(topic, partition, offset, key, value, error))

    def subscribe(self, topics):
        pass  # Stub, do nothing

    def close(self):
        pass  # Stub, do nothing
