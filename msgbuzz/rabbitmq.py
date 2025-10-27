import logging
import multiprocessing
import signal
from functools import partial

import pika
from pika.channel import Channel
from pika.exceptions import AMQPError
from pika.spec import Basic, BasicProperties

from msgbuzz import ConsumerConfirm, MessageBus

_logger = logging.getLogger(__name__)


class RabbitMqMessageBus(MessageBus):

    def __init__(
        self,
        host="localhost",
        port=5672,
        url=None,
        max_priority: int | None = None,
        **kwargs,
    ):
        self.host = host
        self.port = port
        self.url = url
        self.max_priority = max_priority
        self.kwargs = kwargs
        self._subscribers = {}
        self._consumers = []
        self._conn = None
        self.__credentials = None

    @property
    def _conn_params(self):
        if self.url:
            return pika.URLParameters(self.url)
        return pika.ConnectionParameters(
            self.host, self.port, credentials=self._credentials, **self.kwargs
        )

    @property
    def _credentials(self):
        if not self.__credentials:
            username = self.kwargs.pop("username", "guest")
            password = self.kwargs.pop("password", "guest")
            self.__credentials = pika.PlainCredentials(username, password)
        return self.__credentials

    def publish(
        self,
        topic_name,
        message: bytes,
        priority: int = 0,
        persistent: bool = False,
        create_exchange: bool = False,
        **kwargs,
    ):
        try:
            self._publish(
                topic_name,
                message,
                priority=priority,
                persistent=persistent,
                create_exchange=create_exchange,
                **kwargs,
            )
        except AMQPError:
            _logger.info("Connection closed: reconnecting to rabbitmq")
            self._publish(
                topic_name,
                message,
                priority=priority,
                persistent=persistent,
                create_exchange=create_exchange,
                **kwargs,
            )

    def on(self, topic_name, client_group, callback):
        self._subscribers[topic_name] = (client_group, callback)

    def start_consuming(self):
        consumer_count = len(self._subscribers.items())
        if consumer_count == 0:
            return

        # one consumer just use current process
        if consumer_count == 1:
            topic_name = next(iter(self._subscribers))
            client_group, callback = self._subscribers[topic_name]
            consumer = RabbitMqConsumer(
                self._conn_params, topic_name, client_group, callback, self.max_priority
            )
            self._consumers.append(consumer)
            consumer.run()
            return

        # multiple consumers use child process
        for topic_name, (client_group, callback) in self._subscribers.items():
            consumer = RabbitMqConsumer(
                self._conn_params, topic_name, client_group, callback, self.max_priority
            )
            self._consumers.append(consumer)
            consumer.start()

    @property
    def conn(self):
        if not self._conn or self._conn.is_closed:
            self._conn = pika.BlockingConnection(self._conn_params)
        return self._conn

    def _publish(
        self,
        topic_name,
        message,
        priority: int,
        persistent: bool,
        create_exchange: bool,
        **kwargs,
    ):
        channel = self.conn.channel()

        exchange_name = RabbitMqQueueNameGenerator(topic_name, "").exchange_name()
        if create_exchange:
            # create exchange before publish
            # we cannot publish to non existing exchange
            channel.exchange_declare(
                exchange=exchange_name, exchange_type="fanout", durable=True
            )

        channel.basic_publish(
            exchange=exchange_name,
            routing_key="",
            body=message,
            properties=BasicProperties(
                delivery_mode=2 if persistent else 1, priority=priority
            ),
        )
        channel.close()


class RabbitMqConsumer(multiprocessing.Process):

    def __init__(
        self,
        conn_params,
        topic_name,
        client_group,
        callback,
        max_priority: int | None = None,
    ):
        super().__init__()
        self._conn_params = conn_params
        self._topic_name = topic_name
        self._client_group = client_group
        self._name_generator = RabbitMqQueueNameGenerator(topic_name, client_group)
        self._callback = callback
        self._max_priority = max_priority

    def run(self):
        # create new conn
        conn = pika.BlockingConnection(self._conn_params)

        # create channel
        channel = conn.channel()

        prev_sigint_handler = signal.getsignal(signal.SIGINT)
        signal.signal(
            signal.SIGINT,
            partial(_stop_consuming, chan=channel, prev_handler=prev_sigint_handler),
        )
        if hasattr(signal, "SIGTERM"):
            prev_sigterm_handler = signal.getsignal(signal.SIGTERM)
            signal.signal(
                signal.SIGTERM,
                partial(
                    _stop_consuming, chan=channel, prev_handler=prev_sigterm_handler
                ),
            )

        self.register_queues(channel)
        channel.basic_qos(prefetch_count=1)

        # start consuming
        wrapped_callback = _callback_wrapper(conn, self._name_generator, self._callback)
        channel.basic_consume(
            queue=self._name_generator.queue_name(),
            auto_ack=False,
            on_message_callback=wrapped_callback,
        )

        _logger.info(
            f"Waiting incoming message for topic: {self._name_generator.exchange_name()}. To exit press Ctrl+C"
        )
        channel.start_consuming()

        conn.close()

        _logger.info(f"Consumer stopped")

    def register_queues(self, channel):
        q_names = self._name_generator
        # create dlx exchange and queue
        channel.exchange_declare(exchange=q_names.dlx_exchange(), durable=True)
        channel.queue_declare(queue=q_names.dlx_queue_name(), durable=True)
        channel.queue_bind(
            exchange=q_names.dlx_exchange(), queue=q_names.dlx_queue_name()
        )
        # create exchange for pub/sub
        channel.exchange_declare(
            exchange=q_names.exchange_name(), exchange_type="fanout", durable=True
        )
        # create dedicated queue for receiving message (create subscriber)
        channel.queue_declare(
            queue=q_names.queue_name(),
            durable=True,
            arguments={
                "x-dead-letter-exchange": q_names.dlx_exchange(),
                "x-dead-letter-routing-key": q_names.dlx_queue_name(),
                "x-max-priority": self._max_priority,
            },
        )
        # bind created queue with pub/sub exchange
        channel.queue_bind(exchange=q_names.exchange_name(), queue=q_names.queue_name())
        # setup retry requeue exchange and binding
        channel.exchange_declare(exchange=q_names.retry_exchange(), durable=True)
        channel.queue_bind(
            exchange=q_names.retry_exchange(), queue=q_names.queue_name()
        )
        # create retry queue
        channel.queue_declare(
            queue=q_names.retry_queue_name(),
            durable=True,
            arguments={
                "x-dead-letter-exchange": q_names.retry_exchange(),
                "x-dead-letter-routing-key": q_names.queue_name(),
            },
        )

    @staticmethod
    def message_expired(properties):
        return (
            properties.headers
            and properties.headers.get("x-death")
            and properties.headers.get("x-max-retries")
            and properties.headers.get("x-death")[0]["count"]
            > properties.headers.get("x-max-retries")
        )


def _stop_consuming(signum, frame, chan, prev_handler):
    _logger.warning("Stopping consumer...")
    chan.stop_consuming()
    _logger.info("Consumer stopped")

    _signal = signal.Signals(signum)
    if callable(prev_handler):
        prev_handler(signum, frame)
    elif prev_handler == signal.SIG_DFL:
        signal.signal(_signal, signal.SIG_DFL)
        signal.raise_signal(_signal)


class RabbitMqQueueNameGenerator:

    def __init__(self, topic_name, client_group):
        self._client_group = client_group
        self._topic_name = topic_name

    def exchange_name(self):
        return self._topic_name

    def queue_name(self):
        return f"{self._topic_name}.{self._client_group}"

    def retry_exchange(self):
        return self.retry_queue_name()

    def retry_queue_name(self):
        return f"{self.queue_name()}__retry"

    def dlx_exchange(self):
        return self.dlx_queue_name()

    def dlx_queue_name(self):
        return f"{self.queue_name()}__failed"


class RabbitMqConsumerConfirm(ConsumerConfirm):

    def __init__(
        self,
        conn,
        names_gen: RabbitMqQueueNameGenerator,
        channel: Channel,
        delivery: Basic.Deliver,
        properties: BasicProperties,
        body,
    ):
        """
        Create instance of RabbitMqConsumerConfirm

        :param names_gen: QueueNameGenerator
        :param channel:
        :param delivery:
        :param properties:
        :param body:
        """
        self.names_gen = names_gen

        self._channel = channel
        self._delivery = delivery
        self._properties = properties
        self._body = body
        self._conn = conn

    def ack(self):
        def cb():
            self._channel.basic_ack(delivery_tag=self._delivery.delivery_tag)

        self._conn.add_callback_threadsafe(cb)

    def nack(self):
        def cb():
            self._channel.basic_nack(
                delivery_tag=self._delivery.delivery_tag, requeue=False
            )

        self._conn.add_callback_threadsafe(cb)

    def retry(self, delay=60000, max_retries=3):
        def cb():
            # publish to retry queue
            self._properties.expiration = str(delay)
            if self._properties.headers is None:
                self._properties.headers = {}
            self._properties.headers["x-max-retries"] = max_retries  # type: ignore

            q_names = self.names_gen
            self._channel.basic_publish(
                "", q_names.retry_queue_name(), self._body, properties=self._properties
            )

            # ack
            self._channel.basic_ack(delivery_tag=self._delivery.delivery_tag)

        self._conn.add_callback_threadsafe(cb)


def _callback_wrapper(conn, names_gen: RabbitMqQueueNameGenerator, callback):
    """
    Wrapper for callback. since nested function cannot be pickled, we need some top level function to wrap it

    :param callback:
    :return: function
    """

    def fn(ch, method, properties, body):
        if method is None:
            return

        if RabbitMqConsumer.message_expired(properties):
            ch.basic_nack(method.delivery_tag, requeue=False)
            return

        callback(
            RabbitMqConsumerConfirm(conn, names_gen, ch, method, properties, body), body
        )

    return fn
