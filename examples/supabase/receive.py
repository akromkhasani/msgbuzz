import logging
import os

from msgbuzz import ConsumerConfirm
from msgbuzz.supabase import SupabaseMessageBus

logger = logging.getLogger(__name__)


def handle_message(op: ConsumerConfirm, message: bytes):
    logger.info(message.decode("utf-8"))
    op.ack()


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s %(levelname)-5.5s %(name)s: %(message)s",
        level=os.getenv("LOG_LEVEL", "DEBUG").upper(),
    )
    logging.getLogger("hpack").setLevel(logging.ERROR)
    logging.getLogger("httpcore").setLevel(logging.ERROR)
    logging.getLogger("httpx").setLevel(logging.ERROR)

    supabase_url = ""
    supabase_key = ""

    with SupabaseMessageBus(supabase_url, supabase_key) as msg_bus:
        msg_bus.on("topic", "worker", handle_message)
        msg_bus.start_consuming()
