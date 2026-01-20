import logging
import os

from msgbuzz.rabbitmq import RabbitMqMessageBus

logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s %(levelname)-5.5s %(name)s: %(message)s",
        level=os.getenv("LOG_LEVEL", "DEBUG").upper(),
    )
    logging.getLogger("pika").setLevel(logging.ERROR)

    with RabbitMqMessageBus() as msg_bus:
        for _i in range(5):
            i = _i + 1
            logger.debug("Message %d published", i)
            msg_bus.publish("topic", f"Message {i}".encode("utf-8"))
