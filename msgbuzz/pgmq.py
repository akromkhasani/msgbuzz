import concurrent.futures as cf
import logging
import multiprocessing
import signal
import time
from functools import partial
from math import ceil
from urllib.parse import urlparse

from pgmq import Message as PgmqMessage
from pgmq import PGMQueue

from .generic import CallbackType, ConsumerConfirm, MessageBus

_logger = logging.getLogger(__name__)


class Client(PGMQueue):
    def close(self):
        self.pool.close()

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()


class PgmqMessageBus(MessageBus):
    def __init__(
        self, connection_string: str | None = None, message_timeout_seconds: int = 600
    ):
        if connection_string:
            u = urlparse(connection_string)
            db_name = "postgres"
            if (ps := u.path.split("/", 2)) and len(ps) >= 2:
                db_name = ps[1]
            pgmq_client = Client(
                host=u.hostname or "127.0.0.1",
                port=str(u.port) if u.port else "5432",
                database=db_name,
                username=u.username or "postgres",
                password=u.password or "postgres",
            )
        else:
            pgmq_client = Client()

        self.client = pgmq_client
        self.connection_string = connection_string
        self.message_timeout = message_timeout_seconds
        self._subscribers = {}

    def close(self):
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

    def publish(
        self,
        topic_name: str,
        message: bytes,
        create_exchange: bool = False,
        unlogged: bool = False,
        **kwargs,
    ):
        if create_exchange:
            self.client.create_queue(topic_name, unlogged=unlogged)
        self.client.send(topic_name, {"_body": message.decode("utf-8")})

    def on(
        self,
        topic_name: str,
        client_group: str,
        callback: CallbackType,
        workers: int = 1,
        batch_size: int = 1,
        max_threads: int = 1,
        check_interval_seconds: int = 5,
        **kwargs,
    ):
        """
        Register callback fuction as consumer to a topic.

        client_group arg is not used in this implementation.
        """
        self._subscribers[topic_name] = (
            callback,
            max(1, workers),
            max(1, batch_size),
            max(1, max_threads),
            max(1, check_interval_seconds),
        )

    def on2(self, *args, **kwargs):
        self.on(*args, **kwargs)

    def start_consuming(self):
        if not self._subscribers:
            return

        consumers: list[PgmqConsumer] = []
        for topic_name, (
            callback,
            workers,
            batch_size,
            max_threads,
            check_interval,
        ) in self._subscribers.items():
            for _ in range(workers):
                consumer = PgmqConsumer(
                    self.connection_string,
                    topic_name,
                    callback,
                    self.message_timeout,
                    check_interval,
                    batch_size,
                    max_threads,
                )
                consumers.append(consumer)

        # one consumer: use current process
        # many consumers: use subprocesses
        if len(consumers) == 1:
            consumers[0].run()
        else:
            for c in consumers:
                c.start()

            prev_sigint_handler = signal.getsignal(signal.SIGINT)
            signal.signal(
                signal.SIGINT,
                partial(
                    parent_signal_handler,
                    procs=consumers,
                    prev_handler=prev_sigint_handler,
                ),
            )
            if hasattr(signal, "SIGTERM"):
                prev_sigterm_handler = signal.getsignal(signal.SIGTERM)
                signal.signal(
                    signal.SIGTERM,
                    partial(
                        parent_signal_handler,
                        procs=consumers,
                        prev_handler=prev_sigterm_handler,
                    ),
                )


class PgmqConsumerConfirm(ConsumerConfirm):
    def __init__(self, client: Client, topic_name: str, msg_id: int, message: dict):
        self.client = client
        self.topic_name = topic_name
        self.msg_id = msg_id
        self.message = message

    def ack(self):
        self.client.delete(self.topic_name, self.msg_id)

    def nack(self):
        self.client.archive(self.topic_name, self.msg_id)

    def retry(self, delay: int = 60_000, max_retries: int = 3, ack: bool = True):
        """
        Retry the message
        :param delay: delay in milliseconds
        :param max_retries: max retry attempt
        :param ack: ack original message after retry
        """
        delay = max(delay, 0)
        max_retries = max(max_retries, 1)

        _headers = self.message.get("_headers") or dict()
        _headers["x-retry-count"] = (_headers.get("x-retry-count") or 0) + 1
        _headers["x-max-retries"] = max_retries
        self.message["_headers"] = _headers

        self.client.send(
            queue=self.topic_name, message=self.message, delay=ceil(delay / 1000)
        )
        if ack:
            self.ack()


class PgmqConsumer(multiprocessing.Process):
    def __init__(
        self,
        connection_string: str | None,
        topic_name: str,
        callback: CallbackType,
        message_timeout: int,
        check_interval: int,
        batch_size: int,
        max_threads: int,
        unlogged: bool = False,
    ):
        super().__init__()
        self.connection_string = connection_string
        self.topic_name = topic_name
        self.callback = callback
        self.message_timeout = message_timeout
        self.check_interval = check_interval
        self.batch_size = batch_size
        self.max_threads = max_threads
        self.unlogged = unlogged

    def run(self):
        if self.connection_string:
            u = urlparse(self.connection_string)
            db_name = "postgres"
            if (ps := u.path.split("/", 2)) and len(ps) >= 2:
                db_name = ps[1]
            client = Client(
                host=u.hostname or "127.0.0.1",
                port=str(u.port) if u.port else "5432",
                database=db_name,
                username=u.username or "postgres",
                password=u.password or "postgres",
            )
        else:
            client = Client()

        try:
            self.register_queue(client)

            max_workers = min(self.batch_size, self.max_threads)
            process_data_fn = partial(self.process_data, client)

            _logger.info(
                f"Waiting incoming message for topic: {self.topic_name}. To exit press Ctrl+C"
            )

            breaker = {"break": False}
            prev_sigint_handler = signal.getsignal(signal.SIGINT)
            signal.signal(
                signal.SIGINT,
                partial(
                    child_signal_handler,
                    breaker=breaker,
                    prev_handler=prev_sigint_handler,
                ),
            )
            if hasattr(signal, "SIGTERM"):
                prev_sigterm_handler = signal.getsignal(signal.SIGTERM)
                signal.signal(
                    signal.SIGTERM,
                    partial(
                        child_signal_handler,
                        breaker=breaker,
                        prev_handler=prev_sigterm_handler,
                    ),
                )

            with cf.ThreadPoolExecutor(max_workers) as executor:
                while True:
                    if breaker["break"]:
                        break

                    data: list[PgmqMessage] | None = client.read_batch(
                        queue=self.topic_name,
                        vt=self.message_timeout,
                        batch_size=self.batch_size,
                    )

                    if data:
                        list(executor.map(process_data_fn, data))

                    else:
                        time.sleep(self.check_interval)
        finally:
            client.close()

        _logger.info(f"Consumer stopped")

    def register_queue(self, client: Client):
        client.create_queue(self.topic_name, unlogged=self.unlogged)

    def process_data(self, client: Client, msg_obj: PgmqMessage) -> None:
        message = msg_obj.message
        confirm = PgmqConsumerConfirm(client, self.topic_name, msg_obj.msg_id, message)

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


def parent_signal_handler(
    signum,
    frame,
    procs,
    prev_handler=None,
    timeout: int = 5,
):
    _logger.warning("Stopping subprocesses...")
    for p in procs:
        p.terminate()
    for p in procs:
        p.join(timeout)
    for p in procs:
        if p.is_alive():
            p.kill()

    if callable(prev_handler):
        prev_handler(signum, frame)
    elif prev_handler == signal.SIG_DFL:
        _signal = signal.Signals(signum)
        signal.signal(_signal, signal.SIG_DFL)
        signal.raise_signal(_signal)


def child_signal_handler(signum, frame, breaker: dict | None = None, prev_handler=None):
    _logger.warning("Stopping consumer...")
    if breaker is not None:
        breaker["break"] = True

    if callable(prev_handler):
        prev_handler(signum, frame)
    elif prev_handler == signal.SIG_DFL:
        _signal = signal.Signals(signum)
        signal.signal(_signal, signal.SIG_DFL)
        signal.raise_signal(_signal)
