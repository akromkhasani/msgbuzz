import signal
from unittest.mock import MagicMock, call, patch

from msgbuzz.supabase import (
    SupabaseConsumer,
    SupabaseConsumerConfirm,
    SupabaseMessageBus,
    child_signal_handler,
    parent_signal_handler,
)


class TestSupabaseMessageBus:
    def test_init_creates_client_with_pgmq_public_schema(self):
        mock_options = MagicMock()
        with (
            patch(
                "msgbuzz.supabase.ClientOptions", return_value=mock_options
            ) as mock_opts_cls,
            patch("msgbuzz.supabase.create_client") as mock_create_client,
        ):
            bus = SupabaseMessageBus(
                "https://example.supabase.co",
                "anon-key",
                message_timeout_seconds=120,
            )

        mock_opts_cls.assert_called_once_with()
        assert mock_options.schema == "pgmq_public"
        mock_create_client.assert_called_once_with(
            "https://example.supabase.co", "anon-key", mock_options
        )
        assert bus.client is mock_create_client.return_value
        assert bus.supabase_url == "https://example.supabase.co"
        assert bus.supabase_key == "anon-key"
        assert bus.message_timeout == 120
        assert bus._subscribers == {}

    def test_init_default_message_timeout(self):
        with (
            patch("msgbuzz.supabase.ClientOptions"),
            patch("msgbuzz.supabase.create_client"),
        ):
            bus = SupabaseMessageBus("https://example.supabase.co", "anon-key")

        assert bus.message_timeout == 600

    def test_close_and_context_manager(self):
        with (
            patch("msgbuzz.supabase.ClientOptions"),
            patch("msgbuzz.supabase.create_client"),
        ):
            with SupabaseMessageBus("https://example.supabase.co", "anon-key") as bus:
                assert isinstance(bus, SupabaseMessageBus)
                with patch.object(bus, "close", wraps=bus.close) as spy_close:
                    pass
                spy_close.assert_not_called()

            bus.close()

    def test_context_manager_calls_close_on_exit(self):
        with (
            patch("msgbuzz.supabase.ClientOptions"),
            patch("msgbuzz.supabase.create_client"),
        ):
            bus = SupabaseMessageBus("https://example.supabase.co", "anon-key")

        with patch.object(bus, "close") as mock_close:
            with bus as ctx:
                assert ctx is bus

        mock_close.assert_called_once_with()

    def test_publish_calls_send_rpc(self):
        with (
            patch("msgbuzz.supabase.ClientOptions"),
            patch("msgbuzz.supabase.create_client") as mock_create_client,
        ):
            mock_client = mock_create_client.return_value
            bus = SupabaseMessageBus("https://example.supabase.co", "anon-key")
            bus.publish("orders", b'{"id": 1}', extra_arg="ignored")

        mock_client.rpc.assert_called_once_with(
            "send",
            {
                "queue_name": "orders",
                "message": {"_body": '{"id": 1}'},
            },
        )
        mock_client.rpc.return_value.execute.assert_called_once_with()

    def test_on_registers_subscriber_and_clamps_values(self):
        cb = MagicMock()
        with (
            patch("msgbuzz.supabase.ClientOptions"),
            patch("msgbuzz.supabase.create_client"),
        ):
            bus = SupabaseMessageBus("https://example.supabase.co", "anon-key")
            bus.on(
                "events",
                "ignored-group",
                cb,
                workers=0,
                batch_size=-3,
                max_threads=0,
                check_interval_seconds=-5,
                extra="ignored",
            )

        assert bus._subscribers["events"] == (cb, 1, 1, 1, 1)

    def test_on2_delegates_to_on(self):
        cb = MagicMock()
        with (
            patch("msgbuzz.supabase.ClientOptions"),
            patch("msgbuzz.supabase.create_client"),
        ):
            bus = SupabaseMessageBus("https://example.supabase.co", "anon-key")
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
            patch("msgbuzz.supabase.ClientOptions"),
            patch("msgbuzz.supabase.create_client"),
            patch("msgbuzz.supabase.SupabaseConsumer") as mock_consumer_cls,
        ):
            bus = SupabaseMessageBus("https://example.supabase.co", "anon-key")
            result = bus.start_consuming()

        assert result is None
        mock_consumer_cls.assert_not_called()

    def test_start_consuming_single_consumer_runs_in_current_process(self):
        cb = MagicMock()
        with (
            patch("msgbuzz.supabase.ClientOptions"),
            patch("msgbuzz.supabase.create_client"),
            patch("msgbuzz.supabase.SupabaseConsumer") as mock_consumer_cls,
        ):
            bus = SupabaseMessageBus(
                "https://example.supabase.co",
                "anon-key",
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
            "https://example.supabase.co",
            "anon-key",
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
            patch("msgbuzz.supabase.ClientOptions"),
            patch("msgbuzz.supabase.create_client"),
            patch("msgbuzz.supabase.SupabaseConsumer", side_effect=[c1, c2, c3]),
            patch("msgbuzz.supabase.signal.getsignal", return_value=signal.SIG_DFL),
            patch("msgbuzz.supabase.signal.signal") as mock_signal,
        ):
            bus = SupabaseMessageBus("https://example.supabase.co", "anon-key")
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


class TestSupabaseConsumerConfirm:
    def _make_confirm(self, message=None, msg_id=42):
        client = MagicMock()
        msg = {"_body": "hello"} if message is None else message
        confirm = SupabaseConsumerConfirm(client, "orders", msg_id, msg)
        return confirm, client, msg

    def test_ack(self):
        confirm, client, _ = self._make_confirm(msg_id=7)
        confirm.ack()

        client.rpc.assert_called_once_with(
            "delete",
            {"queue_name": "orders", "message_id": 7},
        )
        client.rpc.return_value.execute.assert_called_once_with()

    def test_nack(self):
        confirm, client, _ = self._make_confirm(msg_id=9)
        confirm.nack()

        client.rpc.assert_called_once_with(
            "archive",
            {"queue_name": "orders", "message_id": 9},
        )
        client.rpc.return_value.execute.assert_called_once_with()

    def test_retry_with_ack_and_no_existing_headers(self):
        confirm, client, msg = self._make_confirm(msg_id=11)
        confirm.retry(delay=5500, max_retries=4, ack=True)

        assert msg["_headers"] == {"x-retry-count": 1, "x-max-retries": 4}
        assert client.rpc.call_args_list == [
            call(
                "send",
                {
                    "queue_name": "orders",
                    "message": msg,
                    "sleep_seconds": 6,
                },
            ),
            call(
                "delete",
                {
                    "queue_name": "orders",
                    "message_id": 11,
                },
            ),
        ]
        assert client.rpc.return_value.execute.call_count == 2

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
        client.rpc.assert_called_once_with(
            "send",
            {
                "queue_name": "orders",
                "message": msg,
                "sleep_seconds": 1,
            },
        )
        client.rpc.return_value.execute.assert_called_once_with()

    def test_retry_clamps_negative_delay_and_min_retries(self):
        confirm, client, msg = self._make_confirm(msg_id=13)
        confirm.retry(delay=-500, max_retries=0, ack=True)

        assert msg["_headers"] == {"x-retry-count": 1, "x-max-retries": 1}
        assert client.rpc.call_args_list == [
            call(
                "send",
                {
                    "queue_name": "orders",
                    "message": msg,
                    "sleep_seconds": 0,
                },
            ),
            call(
                "delete",
                {
                    "queue_name": "orders",
                    "message_id": 13,
                },
            ),
        ]


class TestSupabaseConsumer:
    def _make_consumer(self, callback=None):
        return SupabaseConsumer(
            supabase_url="https://example.supabase.co",
            supabase_key="anon-key",
            topic_name="events",
            callback=callback or MagicMock(),
            message_timeout=60,
            check_interval=2,
            batch_size=5,
            max_threads=3,
        )

    def test_message_expired(self):
        assert not SupabaseConsumer.message_expired(None)  # type: ignore[arg-type]
        assert not SupabaseConsumer.message_expired({})
        assert not SupabaseConsumer.message_expired({"_headers": None})
        assert not SupabaseConsumer.message_expired({"_headers": {"x-retry-count": 2}})
        assert not SupabaseConsumer.message_expired({"_headers": {"x-max-retries": 2}})
        assert not SupabaseConsumer.message_expired(
            {"_headers": {"x-retry-count": "3", "x-max-retries": 2}}
        )
        assert not SupabaseConsumer.message_expired(
            {"_headers": {"x-retry-count": 2, "x-max-retries": 2}}
        )
        assert SupabaseConsumer.message_expired(
            {"_headers": {"x-retry-count": 3, "x-max-retries": 2}}
        )

    def test_process_data_valid_message_invokes_callback(self):
        cb = MagicMock()
        client = MagicMock()
        consumer = self._make_consumer(callback=cb)
        msg_obj = {"msg_id": 101, "message": {"_body": "payload"}}

        consumer.process_data(client, msg_obj)

        cb.assert_called_once()
        confirm, body = cb.call_args.args
        assert isinstance(confirm, SupabaseConsumerConfirm)
        assert confirm.msg_id == 101
        assert confirm.topic_name == "events"
        assert body == b"payload"
        client.rpc.assert_not_called()

    def test_process_data_expired_message_nacks_without_callback(self):
        cb = MagicMock()
        client = MagicMock()
        consumer = self._make_consumer(callback=cb)
        msg_obj = {
            "msg_id": 102,
            "message": {
                "_body": "payload",
                "_headers": {"x-retry-count": 4, "x-max-retries": 3},
            },
        }

        consumer.process_data(client, msg_obj)

        cb.assert_not_called()
        client.rpc.assert_called_once_with(
            "archive", {"queue_name": "events", "message_id": 102}
        )
        client.rpc.return_value.execute.assert_called_once_with()

    def test_process_data_invalid_message_format_nacks_without_callback(self):
        cb = MagicMock()
        client = MagicMock()
        consumer = self._make_consumer(callback=cb)
        msg_obj = {"msg_id": 103, "message": {"unexpected": "field"}}

        consumer.process_data(client, msg_obj)

        cb.assert_not_called()
        client.rpc.assert_called_once_with(
            "archive", {"queue_name": "events", "message_id": 103}
        )
        client.rpc.return_value.execute.assert_called_once_with()

    def test_run_processes_batch_sleeps_on_empty_and_stops_on_signal(self):
        cb = MagicMock()
        consumer = self._make_consumer(callback=cb)

        resp_with_data = MagicMock()
        resp_with_data.data = [{"msg_id": 1, "message": {"_body": "hello"}}]
        resp_empty = MagicMock()
        resp_empty.data = []

        mock_client = MagicMock()
        mock_client.rpc.return_value.execute.side_effect = [resp_with_data, resp_empty]
        mock_options = MagicMock()

        captured_handlers = {}

        def fake_signal(sig, handler):
            captured_handlers[sig] = handler

        def fake_sleep(_interval):
            captured_handlers[signal.SIGINT](signal.SIGINT, None)

        with (
            patch("msgbuzz.supabase.ClientOptions", return_value=mock_options),
            patch(
                "msgbuzz.supabase.create_client", return_value=mock_client
            ) as mock_create_client,
            patch("msgbuzz.supabase.signal.getsignal", return_value=None),
            patch("msgbuzz.supabase.signal.signal", side_effect=fake_signal),
            patch("msgbuzz.supabase.time.sleep", side_effect=fake_sleep) as mock_sleep,
        ):
            consumer.run()

        assert mock_options.schema == "pgmq_public"
        mock_create_client.assert_called_once_with(
            "https://example.supabase.co", "anon-key", mock_options
        )
        assert mock_client.rpc.call_args_list == [
            call(
                "read",
                {"queue_name": "events", "sleep_seconds": 60, "n": 5},
            ),
            call(
                "read",
                {"queue_name": "events", "sleep_seconds": 60, "n": 5},
            ),
        ]
        cb.assert_called_once()
        confirm, body = cb.call_args.args
        assert isinstance(confirm, SupabaseConsumerConfirm)
        assert body == b"hello"
        mock_sleep.assert_called_once_with(2)


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
            patch("msgbuzz.supabase.signal.signal") as mock_signal,
            patch("msgbuzz.supabase.signal.raise_signal") as mock_raise,
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

    def test_child_signal_handler_with_none_breaker_and_sig_dfl(self):
        with (
            patch("msgbuzz.supabase.signal.signal") as mock_signal,
            patch("msgbuzz.supabase.signal.raise_signal") as mock_raise,
        ):
            child_signal_handler(
                signal.SIGINT, None, breaker=None, prev_handler=signal.SIG_DFL
            )

        mock_signal.assert_called_once_with(signal.SIGINT, signal.SIG_DFL)
        mock_raise.assert_called_once_with(signal.SIGINT)
