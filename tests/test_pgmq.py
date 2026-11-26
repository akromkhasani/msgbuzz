import signal
from unittest.mock import MagicMock, call, patch

from pgmq import Message as PgmqMessage
from pgmq import PGMQueue

from msgbuzz.pgmq import (
    Client,
    PgmqConsumer,
    PgmqConsumerConfirm,
    PgmqMessageBus,
    child_signal_handler,
    parent_signal_handler,
)


class TestClient:
    def test_close_with_pool(self):
        with patch.object(PGMQueue, "__init__", return_value=None):
            client = Client()
        client.pool = MagicMock()

        client.close()

        client.pool.close.assert_called_once_with()

    def test_close_without_pool(self):
        with patch.object(PGMQueue, "__init__", return_value=None):
            client = Client()
        client.pool = None

        client.close()

    def test_context_manager_closes_on_exit(self):
        with patch.object(PGMQueue, "__init__", return_value=None):
            client = Client()
        client.pool = MagicMock()

        with client as ctx:
            assert ctx is client

        client.pool.close.assert_called_once_with()


class TestPgmqMessageBus:
    def test_init_without_connection_string(self):
        with patch("msgbuzz.pgmq.Client") as mock_client_cls:
            bus = PgmqMessageBus()

        mock_client_cls.assert_called_once_with()
        assert bus.client is mock_client_cls.return_value
        assert bus.connection_string is None
        assert bus.message_timeout == 600
        assert bus._subscribers == {}

    def test_init_with_full_connection_string(self):
        conn_str = "postgresql://alice:secret@db.example.com:5433/appdb"
        with patch("msgbuzz.pgmq.Client") as mock_client_cls:
            bus = PgmqMessageBus(
                connection_string=conn_str, message_timeout_seconds=120
            )

        mock_client_cls.assert_called_once_with(
            host="db.example.com",
            port="5433",
            database="appdb",
            username="alice",
            password="secret",
        )
        assert bus.connection_string == conn_str
        assert bus.message_timeout == 120

    def test_init_with_minimal_connection_string_uses_defaults(self):
        with patch("msgbuzz.pgmq.Client") as mock_client_cls:
            PgmqMessageBus(connection_string="postgresql://")

        mock_client_cls.assert_called_once_with(
            host="127.0.0.1",
            port="5432",
            database="postgres",
            username="postgres",
            password="postgres",
        )

    def test_close_and_context_manager(self):
        with patch("msgbuzz.pgmq.Client") as mock_client_cls:
            mock_client = mock_client_cls.return_value
            with PgmqMessageBus() as bus:
                assert bus.client is mock_client

        mock_client.close.assert_called_once_with()

    def test_publish_without_create_exchange(self):
        with patch("msgbuzz.pgmq.Client") as mock_client_cls:
            mock_client = mock_client_cls.return_value
            bus = PgmqMessageBus()
            bus.publish("orders", b'{"id": 1}')

        mock_client.create_queue.assert_not_called()
        mock_client.send.assert_called_once_with("orders", {"_body": '{"id": 1}'})

    def test_publish_with_create_exchange_and_unlogged(self):
        with patch("msgbuzz.pgmq.Client") as mock_client_cls:
            mock_client = mock_client_cls.return_value
            bus = PgmqMessageBus()
            bus.publish(
                "orders",
                b"hello",
                create_exchange=True,
                unlogged=True,
                extra_arg="ignored",
            )

        mock_client.create_queue.assert_called_once_with("orders", unlogged=True)
        mock_client.send.assert_called_once_with("orders", {"_body": "hello"})

    def test_on_registers_subscriber_and_clamps_values(self):
        cb = MagicMock()
        with patch("msgbuzz.pgmq.Client"):
            bus = PgmqMessageBus()
            bus.on(
                "events",
                "ignored-group",
                cb,
                workers=0,
                batch_size=-3,
                max_threads=0,
                check_interval_seconds=-5,
            )

        assert bus._subscribers["events"] == (cb, 1, 1, 1, 1)

    def test_on2_delegates_to_on(self):
        cb = MagicMock()
        with patch("msgbuzz.pgmq.Client"):
            bus = PgmqMessageBus()
            bus.on2(
                "events",
                "group",
                cb,
                workers=2,
                batch_size=10,
                max_threads=4,
                check_interval_seconds=3,
            )

        assert bus._subscribers["events"] == (cb, 2, 10, 4, 3)

    def test_start_consuming_no_subscribers(self):
        with (
            patch("msgbuzz.pgmq.Client"),
            patch("msgbuzz.pgmq.PgmqConsumer") as mock_consumer_cls,
        ):
            bus = PgmqMessageBus()
            result = bus.start_consuming()

        assert result is None
        mock_consumer_cls.assert_not_called()

    def test_start_consuming_single_consumer_runs_in_current_process(self):
        cb = MagicMock()
        with (
            patch("msgbuzz.pgmq.Client"),
            patch("msgbuzz.pgmq.PgmqConsumer") as mock_consumer_cls,
        ):
            bus = PgmqMessageBus(
                connection_string="postgresql://localhost/db",
                message_timeout_seconds=300,
            )
            bus.on(
                "events",
                "group",
                cb,
                workers=1,
                batch_size=5,
                max_threads=2,
                check_interval_seconds=10,
            )
            bus.start_consuming()

        mock_consumer_cls.assert_called_once_with(
            "postgresql://localhost/db",
            "events",
            cb,
            300,
            10,
            5,
            2,
        )
        consumer = mock_consumer_cls.return_value
        consumer.run.assert_called_once_with()
        consumer.start.assert_not_called()

    def test_start_consuming_multiple_consumers_starts_subprocesses_and_signals(self):
        cb1 = MagicMock()
        cb2 = MagicMock()
        c1, c2, c3 = MagicMock(), MagicMock(), MagicMock()
        with (
            patch("msgbuzz.pgmq.Client"),
            patch("msgbuzz.pgmq.PgmqConsumer", side_effect=[c1, c2, c3]),
            patch("msgbuzz.pgmq.signal.getsignal", return_value=signal.SIG_DFL),
            patch("msgbuzz.pgmq.signal.signal") as mock_signal,
        ):
            bus = PgmqMessageBus()
            bus.on("topic_a", "g", cb1, workers=2)
            bus.on("topic_b", "g", cb2, workers=1)
            bus.start_consuming()

        for c in (c1, c2, c3):
            c.start.assert_called_once_with()
            c.run.assert_not_called()

        registered_signals = [
            call_args.args[0] for call_args in mock_signal.call_args_list
        ]
        assert signal.SIGINT in registered_signals
        if hasattr(signal, "SIGTERM"):
            assert signal.SIGTERM in registered_signals


class TestPgmqConsumerConfirm:
    def _make_confirm(self, message=None, msg_id=42):
        client = MagicMock()
        msg = {"_body": "hello"} if message is None else message
        confirm = PgmqConsumerConfirm(client, "orders", msg_id, msg)
        return confirm, client, msg

    def test_ack(self):
        confirm, client, _ = self._make_confirm(msg_id=7)
        confirm.ack()
        client.delete.assert_called_once_with("orders", 7)

    def test_nack(self):
        confirm, client, _ = self._make_confirm(msg_id=9)
        confirm.nack()
        client.archive.assert_called_once_with("orders", 9)

    def test_retry_with_ack_and_no_existing_headers(self):
        confirm, client, msg = self._make_confirm(msg_id=11)
        confirm.retry(delay=5500, max_retries=4, ack=True)

        assert msg["_headers"] == {"x-retry-count": 1, "x-max-retries": 4}
        client.send.assert_called_once_with(queue="orders", message=msg, delay=6)
        client.delete.assert_called_once_with("orders", 11)

    def test_retry_without_ack_preserves_existing_headers_and_increments(self):
        confirm, client, msg = self._make_confirm(
            msg_id=12,
            message={
                "_body": "hello",
                "_headers": {"custom": "val", "x-retry-count": 2},
            },
        )
        confirm.retry(delay=1000, max_retries=5, ack=False)

        assert msg["_headers"] == {
            "custom": "val",
            "x-retry-count": 3,
            "x-max-retries": 5,
        }
        client.send.assert_called_once_with(queue="orders", message=msg, delay=1)
        client.delete.assert_not_called()

    def test_retry_clamps_negative_delay_and_min_retries(self):
        confirm, client, msg = self._make_confirm(msg_id=13)
        confirm.retry(delay=-500, max_retries=0, ack=True)

        assert msg["_headers"] == {"x-retry-count": 1, "x-max-retries": 1}
        client.send.assert_called_once_with(queue="orders", message=msg, delay=0)
        client.delete.assert_called_once_with("orders", 13)


class TestPgmqConsumer:
    def _make_consumer(self, connection_string=None, callback=None, unlogged=False):
        return PgmqConsumer(
            connection_string=connection_string,
            topic_name="events",
            callback=callback or MagicMock(),
            message_timeout=60,
            check_interval=2,
            batch_size=5,
            max_threads=3,
            unlogged=unlogged,
        )

    def test_register_queue(self):
        client = MagicMock()
        consumer = self._make_consumer(unlogged=True)

        consumer.register_queue(client)

        client.create_queue.assert_called_once_with("events", unlogged=True)

    def test_message_expired(self):
        assert not PgmqConsumer.message_expired(None)  # type: ignore[arg-type]
        assert not PgmqConsumer.message_expired({})
        assert not PgmqConsumer.message_expired({"_headers": None})
        assert not PgmqConsumer.message_expired({"_headers": {"x-retry-count": 2}})
        assert not PgmqConsumer.message_expired({"_headers": {"x-max-retries": 2}})
        assert not PgmqConsumer.message_expired(
            {"_headers": {"x-retry-count": "3", "x-max-retries": 2}}
        )
        assert not PgmqConsumer.message_expired(
            {"_headers": {"x-retry-count": 2, "x-max-retries": 2}}
        )
        assert PgmqConsumer.message_expired(
            {"_headers": {"x-retry-count": 3, "x-max-retries": 2}}
        )

    def test_process_data_valid_message_invokes_callback(self):
        cb = MagicMock()
        client = MagicMock()
        consumer = self._make_consumer(callback=cb)
        msg_obj = MagicMock(spec=PgmqMessage)
        msg_obj.msg_id = 101
        msg_obj.message = {"_body": "payload"}

        consumer.process_data(client, msg_obj)

        cb.assert_called_once()
        confirm, body = cb.call_args.args
        assert isinstance(confirm, PgmqConsumerConfirm)
        assert confirm.msg_id == 101
        assert confirm.topic_name == "events"
        assert body == b"payload"
        client.archive.assert_not_called()

    def test_process_data_expired_message_nacks_without_callback(self):
        cb = MagicMock()
        client = MagicMock()
        consumer = self._make_consumer(callback=cb)
        msg_obj = MagicMock(spec=PgmqMessage)
        msg_obj.msg_id = 102
        msg_obj.message = {
            "_body": "payload",
            "_headers": {"x-retry-count": 4, "x-max-retries": 3},
        }

        consumer.process_data(client, msg_obj)

        cb.assert_not_called()
        client.archive.assert_called_once_with("events", 102)

    def test_process_data_invalid_message_format_nacks_without_callback(self):
        cb = MagicMock()
        client = MagicMock()
        consumer = self._make_consumer(callback=cb)
        msg_obj = MagicMock(spec=PgmqMessage)
        msg_obj.msg_id = 103
        msg_obj.message = {"unexpected": "field"}

        consumer.process_data(client, msg_obj)

        cb.assert_not_called()
        client.archive.assert_called_once_with("events", 103)

    def test_run_processes_batch_sleeps_on_empty_and_closes_on_signal(self):
        cb = MagicMock()
        consumer = self._make_consumer(
            connection_string="postgresql://u:p@dbhost:5434/mydb",
            callback=cb,
        )

        msg_obj = MagicMock(spec=PgmqMessage)
        msg_obj.msg_id = 1
        msg_obj.message = {"_body": "hello"}

        mock_client = MagicMock()
        mock_client.read_batch.side_effect = [[msg_obj], []]

        captured_handlers = {}

        def fake_signal(sig, handler):
            captured_handlers[sig] = handler

        def fake_sleep(_interval):
            captured_handlers[signal.SIGINT](signal.SIGINT, None)

        with (
            patch("msgbuzz.pgmq.Client", return_value=mock_client) as mock_client_cls,
            patch("msgbuzz.pgmq.signal.getsignal", return_value=None),
            patch("msgbuzz.pgmq.signal.signal", side_effect=fake_signal),
            patch("msgbuzz.pgmq.time.sleep", side_effect=fake_sleep) as mock_sleep,
        ):
            consumer.run()

        mock_client_cls.assert_called_once_with(
            host="dbhost",
            port="5434",
            database="mydb",
            username="u",
            password="p",
        )
        mock_client.create_queue.assert_called_once_with("events", unlogged=False)
        assert mock_client.read_batch.call_args_list == [
            call(queue="events", vt=60, batch_size=5),
            call(queue="events", vt=60, batch_size=5),
        ]
        cb.assert_called_once()
        mock_sleep.assert_called_once_with(2)
        mock_client.close.assert_called_once_with()

    def test_run_without_connection_string_uses_default_client(self):
        consumer = self._make_consumer(connection_string=None)
        mock_client = MagicMock()
        mock_client.read_batch.return_value = []
        captured_handlers = {}

        def fake_signal(sig, handler):
            captured_handlers[sig] = handler

        def fake_sleep(_interval):
            captured_handlers[signal.SIGINT](signal.SIGINT, None)

        with (
            patch("msgbuzz.pgmq.Client", return_value=mock_client) as mock_client_cls,
            patch("msgbuzz.pgmq.signal.getsignal", return_value=None),
            patch("msgbuzz.pgmq.signal.signal", side_effect=fake_signal),
            patch("msgbuzz.pgmq.time.sleep", side_effect=fake_sleep),
        ):
            consumer.run()

        mock_client_cls.assert_called_once_with()
        mock_client.close.assert_called_once_with()


class TestSignalHandlers:
    def test_parent_signal_handler_terminates_kills_and_calls_prev(self):
        alive_proc = MagicMock()
        alive_proc.is_alive.return_value = True
        dead_proc = MagicMock()
        dead_proc.is_alive.return_value = False
        prev_handler = MagicMock()

        parent_signal_handler(
            signal.SIGINT,
            None,
            procs=[alive_proc, dead_proc],
            prev_handler=prev_handler,
            timeout=3,
        )

        alive_proc.terminate.assert_called_once_with()
        dead_proc.terminate.assert_called_once_with()
        alive_proc.join.assert_called_once_with(3)
        dead_proc.join.assert_called_once_with(3)
        alive_proc.kill.assert_called_once_with()
        dead_proc.kill.assert_not_called()
        prev_handler.assert_called_once_with(signal.SIGINT, None)

    def test_parent_signal_handler_reraises_when_sig_dfl(self):
        proc = MagicMock()
        proc.is_alive.return_value = False

        with (
            patch("msgbuzz.pgmq.signal.signal") as mock_signal,
            patch("msgbuzz.pgmq.signal.raise_signal") as mock_raise,
        ):
            parent_signal_handler(
                signal.SIGINT,
                None,
                procs=[proc],
                prev_handler=signal.SIG_DFL,
            )

        mock_signal.assert_called_once_with(signal.SIGINT, signal.SIG_DFL)
        mock_raise.assert_called_once_with(signal.SIGINT)

    def test_child_signal_handler_sets_breaker_and_calls_prev(self):
        breaker = {"break": False}
        prev_handler = MagicMock()

        child_signal_handler(
            signal.SIGINT, None, breaker=breaker, prev_handler=prev_handler
        )

        assert breaker["break"] is True
        prev_handler.assert_called_once_with(signal.SIGINT, None)

    def test_child_signal_handler_reraises_when_sig_dfl(self):
        breaker = {"break": False}

        with (
            patch("msgbuzz.pgmq.signal.signal") as mock_signal,
            patch("msgbuzz.pgmq.signal.raise_signal") as mock_raise,
        ):
            child_signal_handler(
                signal.SIGINT, None, breaker=breaker, prev_handler=signal.SIG_DFL
            )

        assert breaker["break"] is True
        mock_signal.assert_called_once_with(signal.SIGINT, signal.SIG_DFL)
        mock_raise.assert_called_once_with(signal.SIGINT)
