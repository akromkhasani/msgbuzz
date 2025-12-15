import concurrent.futures as cf
import logging
import multiprocessing
import signal
import time
from collections.abc import Sequence
from functools import partial
from math import ceil
from typing import Callable

from supabase import Client, ClientOptions, create_client

from . import ConsumerConfirm, MessageBus

_logger = logging.getLogger(__name__)

CallbackType = Callable[[ConsumerConfirm, bytes], None]


class SupabaseMessageBus(MessageBus):
    def __init__(
        self, supabase_url: str, supabase_key: str, message_timeout_seconds: int = 600
    ):
        client_options = ClientOptions()
        client_options.schema = "pgmq_public"
        self.client = create_client(supabase_url, supabase_key, client_options)
        self.supabase_url = supabase_url
        self.supabase_key = supabase_key
        self.message_timeout = message_timeout_seconds
        self._subscribers = {}

    def publish(self, topic_name: str, message: bytes, **kwargs):
        self.client.rpc(
            "send",
            {
                "queue_name": topic_name,
                "message": {"_body": message.decode("utf-8")},
            },
        ).execute()

    def on(
        self,
        topic_name: str,
        client_group: str,
        callback: CallbackType,
        check_interval_seconds: int = 5,
        batch_size: int = 1,
        max_threads: int = 5,
        **kwargs,
    ):
        self._subscribers[topic_name] = (
            callback,
            check_interval_seconds,
            max(1, batch_size),
            max(1, max_threads),
        )

    def on2(self, *args, **kwargs):
        # TODO: implement on2
        self.on(*args, **kwargs)

    def start_consuming(self):
        consumer_count = len(self._subscribers)
        if consumer_count == 0:
            return

        for topic_name, (
            callback,
            check_interval,
            batch_size,
            max_threads,
        ) in self._subscribers.items():
            consumer = SupabaseConsumer(
                self.supabase_url,
                self.supabase_key,
                topic_name,
                callback,
                self.message_timeout,
                check_interval,
                batch_size,
                max_threads,
            )
            if consumer_count == 1:
                # one consumer just use current process
                consumer.run()
            else:
                # multiple consumers use child process
                consumer.start()


class SupabaseConsumer(multiprocessing.Process):
    def __init__(
        self,
        supabase_url: str,
        supabase_key: str,
        topic_name: str,
        callback: CallbackType,
        message_timeout: int,
        check_interval: int,
        batch_size: int,
        max_threads: int,
    ):
        super().__init__()
        self.supabase_url = supabase_url
        self.supabase_key = supabase_key
        self.topic_name = topic_name
        self.callback = callback
        self.message_timeout = message_timeout
        self.check_interval = check_interval
        self.batch_size = batch_size
        self.max_threads = max_threads

    def run(self):
        client_options = ClientOptions()
        client_options.schema = "pgmq_public"
        client = create_client(self.supabase_url, self.supabase_key, client_options)

        max_workers = min(self.batch_size, self.max_threads)
        process_data_fn = partial(self.process_data, client)

        _logger.info(
            f"Waiting incoming message for topic: {self.topic_name}. To exit press Ctrl+C"
        )

        breaker = {"break": False}
        signal.signal(signal.SIGTERM, partial(signal_handler, breaker=breaker))
        signal.signal(signal.SIGINT, partial(signal_handler, breaker=breaker))

        while True:
            if breaker["break"]:
                break

            resp = client.rpc(
                "read",
                {
                    "queue_name": self.topic_name,
                    "sleep_seconds": self.message_timeout,
                    "n": self.batch_size,
                },
            ).execute()

            if resp.data and isinstance(resp.data, Sequence):
                with cf.ThreadPoolExecutor(max_workers) as executor:
                    list(executor.map(process_data_fn, resp.data))

            else:
                time.sleep(self.check_interval)

        _logger.info(f"Consumer stopped")

    def process_data(self, client: Client, msg_obj: dict) -> None:
        message = msg_obj["message"]
        confirm = SupabaseConsumerConfirm(
            client, self.topic_name, msg_obj["msg_id"], message
        )

        if self.message_expired(message):
            return confirm.nack()

        try:
            actual_message = message["_body"].encode("utf-8")
        except Exception:
            _logger.warning("Invalid message format")
            return confirm.nack()

        self.callback(confirm, actual_message)

    @staticmethod
    def message_expired(message: dict) -> bool:
        if isinstance(message, dict) and (
            _headers := message.get("_headers") or dict()
        ):
            retry_count = _headers.get("x-retry-count")
            max_retries = _headers.get("x-max-retries")
            if isinstance(retry_count, int) and isinstance(max_retries, int):
                return retry_count > max_retries
        return False


def signal_handler(signum, frame, breaker: dict):
    _logger.warning(f"Received signal {signal.Signals(signum).name}")
    if signum in (signal.SIGTERM, signal.SIGINT):
        breaker["break"] = True


class SupabaseConsumerConfirm(ConsumerConfirm):
    def __init__(self, client: Client, topic_name: str, msg_id: int, message: dict):
        self.client = client
        self.topic_name = topic_name
        self.msg_id = msg_id
        self.message = message

    def ack(self):
        self.client.rpc(
            "delete",
            {
                "queue_name": self.topic_name,
                "message_id": self.msg_id,
            },
        ).execute()

    def nack(self):
        self.client.rpc(
            "archive",
            {
                "queue_name": self.topic_name,
                "message_id": self.msg_id,
            },
        ).execute()

    def retry(self, delay: int = 60_000, max_retries: int = 3, ack: bool = True):
        delay = max(delay, 0)
        max_retries = max(max_retries, 1)

        _headers = self.message.get("_headers") or dict()
        _headers["x-retry-count"] = (_headers.get("x-retry-count") or 0) + 1
        _headers["x-max-retries"] = max_retries
        self.message["_headers"] = _headers

        self.client.rpc(
            "send",
            {
                "queue_name": self.topic_name,
                "message": self.message,
                "sleep_seconds": ceil(delay / 1000),
            },
        ).execute()
        if ack:
            self.ack()
        return
