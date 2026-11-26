from unittest.mock import MagicMock, call, patch

import pika
import pytest
from pika.exceptions import AMQPError
from pika.spec import Basic, BasicProperties

from msgbuzz.rabbitmq import (
    RabbitMqConsumer,
    RabbitMqConsumer2,
    RabbitMqConsumerConfirm,
    RabbitMqMessageBus,
    RabbitMqQueueNameGenerator,
)


class TestRabbitMqQueueNameGenerator:
    def test_names(self):
        gen = RabbitMqQueueNameGenerator("orders", "billing")
        assert gen.exchange_name() == "orders"
        assert gen.queue_name() == "orders.billing"
        assert gen.retry_queue_name() == "orders.billing__retry"
        assert gen.retry_exchange() == "orders.billing__retry"
        assert gen.dlx_queue_name() == "orders.billing__failed"
        assert gen.dlx_exchange() == "orders.billing__failed"


class TestRabbitMqConsumerConfirm:
    def _make_confirm(self, delivery_tag=42, headers=None):
        conn = MagicMock()
        conn.add_callback_threadsafe.side_effect = lambda cb: cb()
        names_gen = RabbitMqQueueNameGenerator("topic", "group")
        channel = MagicMock()
        delivery = Basic.Deliver(delivery_tag=delivery_tag)
        properties = BasicProperties(headers=headers)
        body = b"payload"
        confirm = RabbitMqConsumerConfirm(
            conn, names_gen, channel, delivery, properties, body
        )
        return confirm, conn, channel, delivery, properties

    def test_ack(self):
        confirm, conn, channel, _, _ = self._make_confirm(delivery_tag=7)
        confirm.ack()

        conn.add_callback_threadsafe.assert_called_once()
        channel.basic_ack.assert_called_once_with(delivery_tag=7)

    def test_ack_requires_int_delivery_tag(self):
        confirm, *_ = self._make_confirm(delivery_tag=None)
        with pytest.raises(AssertionError):
            confirm.ack()

    def test_nack(self):
        confirm, conn, channel, _, _ = self._make_confirm(delivery_tag=9)
        confirm.nack()

        conn.add_callback_threadsafe.assert_called_once()
        channel.basic_nack.assert_called_once_with(delivery_tag=9, requeue=False)

    def test_nack_requires_int_delivery_tag(self):
        confirm, *_ = self._make_confirm(delivery_tag=None)
        with pytest.raises(AssertionError):
            confirm.nack()

    def test_retry_with_ack_and_no_existing_headers(self):
        confirm, conn, channel, _, properties = self._make_confirm(
            delivery_tag=11, headers=None
        )
        confirm.retry(delay=5000, max_retries=4, ack=True)

        conn.add_callback_threadsafe.assert_called_once()
        assert properties.expiration == "5000"
        assert properties.headers == {"x-max-retries": 4}
        channel.basic_publish.assert_called_once_with(
            "", "topic.group__retry", b"payload", properties=properties
        )
        channel.basic_ack.assert_called_once_with(delivery_tag=11)

    def test_retry_without_ack_preserves_existing_headers(self):
        confirm, conn, channel, _, properties = self._make_confirm(
            delivery_tag=None, headers={"custom": "value"}
        )
        confirm.retry(delay=1000, max_retries=2, ack=False)

        conn.add_callback_threadsafe.assert_called_once()
        assert properties.expiration == "1000"
        assert properties.headers == {"custom": "value", "x-max-retries": 2}
        channel.basic_publish.assert_called_once_with(
            "", "topic.group__retry", b"payload", properties=properties
        )
        channel.basic_ack.assert_not_called()

    def test_retry_with_ack_requires_int_delivery_tag(self):
        confirm, *_ = self._make_confirm(delivery_tag=None)
        with pytest.raises(AssertionError):
            confirm.retry(ack=True)


class TestRabbitMqConsumer:
    def test_message_expired(self):
        assert not RabbitMqConsumer.message_expired(BasicProperties(headers=None))
        assert not RabbitMqConsumer.message_expired(BasicProperties(headers={}))
        assert not RabbitMqConsumer.message_expired(
            BasicProperties(headers={"x-death": [{"count": 2}]})
        )
        assert not RabbitMqConsumer.message_expired(
            BasicProperties(headers={"x-max-retries": 2})
        )
        assert not RabbitMqConsumer.message_expired(
            BasicProperties(headers={"x-death": [{"count": 2}], "x-max-retries": 2})
        )
        assert RabbitMqConsumer.message_expired(
            BasicProperties(headers={"x-death": [{"count": 3}], "x-max-retries": 2})
        )

    def test_register_queues_default_and_priority(self):
        conn = MagicMock()
        channel = MagicMock()
        conn.channel.return_value = channel

        consumer = RabbitMqConsumer(
            MagicMock(), "events", "worker", MagicMock(), max_priority=10
        )
        consumer.register_queues(conn)

        channel.exchange_declare.assert_has_calls(
            [
                call(exchange="events.worker__failed", durable=True),
                call(exchange="events", exchange_type="fanout", durable=True),
                call(exchange="events.worker__retry", durable=True),
            ]
        )
        channel.queue_declare.assert_has_calls(
            [
                call(queue="events.worker__failed", durable=True),
                call(
                    queue="events.worker",
                    durable=True,
                    arguments={
                        "x-dead-letter-exchange": "events.worker__failed",
                        "x-dead-letter-routing-key": "events.worker__failed",
                        "x-max-priority": 10,
                    },
                ),
                call(
                    queue="events.worker__retry",
                    durable=True,
                    arguments={
                        "x-dead-letter-exchange": "events.worker__retry",
                        "x-dead-letter-routing-key": "events.worker",
                    },
                ),
            ]
        )
        channel.queue_bind.assert_has_calls(
            [
                call(
                    exchange="events.worker__failed",
                    queue="events.worker__failed",
                ),
                call(exchange="events", queue="events.worker"),
                call(exchange="events.worker__retry", queue="events.worker"),
            ]
        )
        channel.close.assert_called_once()

    def test_register_queues_reopens_closed_channel_on_queue_declare_error(self):
        conn = MagicMock()
        first_channel = MagicMock()
        second_channel = MagicMock()
        first_channel.is_closed = True

        def queue_declare_side_effect(**kwargs):
            if kwargs.get("queue") == "events.worker":
                raise AMQPError("PRECONDITION_FAILED")

        first_channel.queue_declare.side_effect = queue_declare_side_effect
        conn.channel.side_effect = [first_channel, second_channel]

        consumer = RabbitMqConsumer(MagicMock(), "events", "worker", MagicMock())
        consumer.register_queues(conn)

        assert conn.channel.call_count == 2
        second_channel.queue_bind.assert_any_call(
            exchange="events", queue="events.worker"
        )
        second_channel.close.assert_called_once()

    @patch("msgbuzz.rabbitmq.signal.signal")
    @patch("msgbuzz.rabbitmq.signal.getsignal", return_value=None)
    @patch("msgbuzz.rabbitmq.pika.BlockingConnection")
    def test_run_consumes_messages_and_handles_expiry(
        self, mock_blocking_conn, _mock_getsignal, _mock_signal
    ):
        conn = MagicMock()
        channel = MagicMock()
        conn.channel.return_value = channel
        mock_blocking_conn.return_value = conn

        received = []

        def callback(confirm, body):
            received.append((confirm, body))

        consumer = RabbitMqConsumer(MagicMock(), "events", "worker", callback)

        with patch.object(consumer, "register_queues") as mock_register:
            consumer.run()

        mock_register.assert_called_once_with(conn)
        channel.basic_qos.assert_called_once_with(prefetch_count=1)
        channel.basic_consume.assert_called_once()
        consume_kwargs = channel.basic_consume.call_args.kwargs
        assert consume_kwargs["queue"] == "events.worker"
        assert consume_kwargs["auto_ack"] is False

        wrapped_cb = consume_kwargs["on_message_callback"]
        # None method is ignored
        wrapped_cb(channel, None, BasicProperties(), b"ignored")
        assert received == []

        # Expired message is nacked
        expired_props = BasicProperties(
            headers={"x-death": [{"count": 4}], "x-max-retries": 3}
        )
        wrapped_cb(channel, Basic.Deliver(delivery_tag=5), expired_props, b"expired")
        channel.basic_nack.assert_called_once_with(5, requeue=False)
        assert received == []

        # Valid message invokes callback with RabbitMqConsumerConfirm
        valid_props = BasicProperties()
        wrapped_cb(channel, Basic.Deliver(delivery_tag=6), valid_props, b"hello")
        assert len(received) == 1
        assert isinstance(received[0][0], RabbitMqConsumerConfirm)
        assert received[0][1] == b"hello"

        channel.start_consuming.assert_called_once()
        conn.close.assert_called_once()


class TestRabbitMqConsumer2:
    @patch("msgbuzz.rabbitmq.sleep")
    @patch("msgbuzz.rabbitmq.signal.signal")
    @patch("msgbuzz.rabbitmq.signal.getsignal", return_value=None)
    @patch("msgbuzz.rabbitmq.pika.BlockingConnection")
    def test_run_polls_and_processes_messages(
        self, mock_blocking_conn, _mock_getsignal, mock_signal, mock_sleep
    ):
        conn = MagicMock()
        channel = MagicMock()
        conn.channel.return_value = channel
        mock_blocking_conn.return_value = conn

        registered_handlers = []

        def capture_signal(_sig, handler):
            registered_handlers.append(handler)

        mock_signal.side_effect = capture_signal

        expired_props = BasicProperties(
            headers={"x-death": [{"count": 2}], "x-max-retries": 1}
        )
        valid_props = BasicProperties()

        call_count = 0

        def basic_get_side_effect(_queue):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return (None, None, None)
            if call_count == 2:
                return (Basic.GetOk(delivery_tag=1), expired_props, b"old")
            if call_count == 3:
                return (Basic.GetOk(delivery_tag=2), valid_props, b"fresh")
            return None

        channel.basic_get.side_effect = basic_get_side_effect

        received = []

        def callback(confirm, body):
            received.append((confirm, body))
            # Trigger the registered SIGINT handler to break the loop cleanly
            registered_handlers[0](2, None)

        consumer = RabbitMqConsumer2(MagicMock(), "events", "worker", callback)
        with patch.object(consumer, "register_queues") as mock_register:
            consumer.run()

        mock_register.assert_called_once_with(conn)
        mock_sleep.assert_called_once_with(1)
        channel.basic_nack.assert_called_once_with(1, requeue=False)
        assert len(received) == 1
        assert isinstance(received[0][0], RabbitMqConsumerConfirm)
        assert received[0][1] == b"fresh"
        conn.close.assert_called_once()


class TestRabbitMqMessageBus:
    def test_conn_property_creates_and_reuses_connection(self):
        with patch("msgbuzz.rabbitmq.pika.BlockingConnection") as mock_conn_cls:
            mock_conn = MagicMock()
            mock_conn.is_closed = False
            mock_conn_cls.return_value = mock_conn

            bus = RabbitMqMessageBus(
                host="rabbit.local", port=5673, username="alice", password="secret"
            )
            assert bus.conn is mock_conn
            assert bus.conn is mock_conn
            mock_conn_cls.assert_called_once()

            conn_params = mock_conn_cls.call_args.args[0]
            assert isinstance(conn_params, pika.ConnectionParameters)
            assert conn_params.host == "rabbit.local"
            assert conn_params.port == 5673
            assert isinstance(conn_params.credentials, pika.PlainCredentials)
            assert conn_params.credentials.username == "alice"
            assert conn_params.credentials.password == "secret"

            # Reconnects when closed
            mock_conn.is_closed = True
            new_conn = MagicMock()
            new_conn.is_closed = False
            mock_conn_cls.return_value = new_conn
            assert bus.conn is new_conn
            assert mock_conn_cls.call_count == 2

    def test_conn_property_with_url(self):
        with patch("msgbuzz.rabbitmq.pika.BlockingConnection") as mock_conn_cls:
            mock_conn = MagicMock()
            mock_conn.is_closed = False
            mock_conn_cls.return_value = mock_conn

            bus = RabbitMqMessageBus(url="amqp://user:pass@host:5672/vhost")
            _ = bus.conn
            conn_params = mock_conn_cls.call_args.args[0]
            assert isinstance(conn_params, pika.URLParameters)

    def test_context_manager_and_close(self):
        bus = RabbitMqMessageBus()
        with patch.object(bus, "close") as mock_close:
            with bus as entered:
                assert entered is bus
            mock_close.assert_called_once()
        # Calling real close() does not raise
        bus.close()

    def test_publish_with_options_and_create_exchange(self):
        with patch("msgbuzz.rabbitmq.pika.BlockingConnection") as mock_conn_cls:
            mock_conn = MagicMock()
            mock_conn.is_closed = False
            mock_channel = MagicMock()
            mock_conn.channel.return_value = mock_channel
            mock_conn_cls.return_value = mock_conn

            bus = RabbitMqMessageBus()
            bus.publish(
                "orders",
                b"hello",
                priority=5,
                persistent=True,
                create_exchange=True,
                content_type="application/json",
                content_encoding="utf-8",
                headers={"x-trace": "1"},
                expiration="30000",
            )

            mock_channel.exchange_declare.assert_called_once_with(
                exchange="orders", exchange_type="fanout", durable=True
            )
            mock_channel.basic_publish.assert_called_once()
            pub_kwargs = mock_channel.basic_publish.call_args.kwargs
            assert pub_kwargs["exchange"] == "orders"
            assert pub_kwargs["routing_key"] == ""
            assert pub_kwargs["body"] == b"hello"
            props = pub_kwargs["properties"]
            assert props.delivery_mode == 2
            assert props.priority == 5
            assert props.content_type == "application/json"
            assert props.content_encoding == "utf-8"
            assert props.headers == {"x-trace": "1"}
            assert props.expiration == "30000"
            mock_channel.close.assert_called_once()

    def test_publish_retries_once_on_amqp_error(self):
        bus = RabbitMqMessageBus()
        with patch.object(
            bus, "_publish", side_effect=[AMQPError("closed"), None]
        ) as mock_pub:
            bus.publish("orders", b"msg")
            assert mock_pub.call_count == 2

    def test_publish_raises_if_retry_also_fails(self):
        bus = RabbitMqMessageBus()
        with patch.object(
            bus, "_publish", side_effect=[AMQPError("err1"), AMQPError("err2")]
        ):
            with pytest.raises(AMQPError, match="err2"):
                bus.publish("orders", b"msg")

    def test_on_and_on2_register_subscribers(self):
        bus = RabbitMqMessageBus()
        cb1 = MagicMock()
        cb2 = MagicMock()

        bus.on("topic1", "group1", cb1, workers=0, max_priority=5)
        bus.on2("topic2", "group2", cb2, workers=3, max_priority=None)

        assert bus._subscribers["topic1"] == ("group1", cb1, 1, 5)
        assert bus._subscribers2["topic2"] == ("group2", cb2, 3, None)

    def test_start_consuming_no_subscribers(self):
        bus = RabbitMqMessageBus()
        bus.start_consuming()
        assert bus._consumers == []

    def test_start_consuming_single_consumer_runs_in_current_process(self):
        bus = RabbitMqMessageBus()
        cb = MagicMock()
        bus.on("topic1", "group1", cb, workers=1)

        with patch("msgbuzz.rabbitmq.RabbitMqConsumer") as mock_consumer_cls:
            mock_consumer = MagicMock()
            mock_consumer_cls.return_value = mock_consumer

            bus.start_consuming()

            mock_consumer_cls.assert_called_once()
            mock_consumer.run.assert_called_once()
            mock_consumer.start.assert_not_called()

    @patch("msgbuzz.rabbitmq.signal.signal")
    @patch("msgbuzz.rabbitmq.signal.getsignal", return_value=None)
    def test_start_consuming_multiple_consumers_starts_subprocesses(
        self, _mock_getsignal, mock_signal
    ):
        bus = RabbitMqMessageBus()
        cb1 = MagicMock()
        cb2 = MagicMock()
        bus.on("topic1", "group1", cb1, workers=1)
        bus.on2("topic2", "group2", cb2, workers=2)

        with (
            patch("msgbuzz.rabbitmq.RabbitMqConsumer") as mock_c1_cls,
            patch("msgbuzz.rabbitmq.RabbitMqConsumer2") as mock_c2_cls,
        ):
            c1 = MagicMock()
            c2_a = MagicMock()
            c2_b = MagicMock()
            mock_c1_cls.return_value = c1
            mock_c2_cls.side_effect = [c2_a, c2_b]

            bus.start_consuming()

            assert len(bus._consumers) == 3
            c1.start.assert_called_once()
            c2_a.start.assert_called_once()
            c2_b.start.assert_called_once()
            c1.run.assert_not_called()
            assert mock_signal.call_count >= 1
