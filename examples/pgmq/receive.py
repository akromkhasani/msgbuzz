import logging
import os

from msgbuzz import ConsumerConfirm
from msgbuzz.pgmq import PgmqMessageBus

logger = logging.getLogger(__name__)


def handle_message(op: ConsumerConfirm, message: bytes):
    logger.info(message.decode("utf-8"))
    op.ack()


if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s %(levelname)-5.5s %(name)s: %(message)s",
        level=os.getenv("LOG_LEVEL", "DEBUG").upper(),
    )
    logging.getLogger("psycopg").setLevel(logging.ERROR)

    with PgmqMessageBus() as msg_bus:
        msg_bus.on("topic", "worker", handle_message)
        msg_bus.start_consuming()
