from abc import abstractmethod
from typing import Any, Callable


class ConsumerConfirm:

    @abstractmethod
    def ack(self):
        pass

    @abstractmethod
    def nack(self):
        pass

    @abstractmethod
    def retry(self, delay: int = 60_000, max_retries: int = 3, ack: bool = True):
        """
        Retry the message
        :param delay: delay in milliseconds
        :param max_retries: max retry attempt
        :param ack: ack original message after retry
        """
        pass


CallbackType = Callable[[ConsumerConfirm, bytes], None]


class MessageBus:
    @abstractmethod
    def close(self):
        pass

    @abstractmethod
    def __enter__(self) -> Any:
        pass

    @abstractmethod
    def __exit__(self, *args, **kwargs):
        pass

    @abstractmethod
    def publish(self, topic_name: str, message: bytes, **kwargs):
        pass

    @abstractmethod
    def on(self, topic_name: str, client_group: str, callback: CallbackType, **kwargs):
        pass

    @abstractmethod
    def on2(self, topic_name: str, client_group: str, callback: CallbackType, **kwargs):
        pass

    @abstractmethod
    def start_consuming(self):
        pass
