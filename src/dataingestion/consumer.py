# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Consume order events from the Kafka `orders` topic.

DPFMD-8: Setup Python consumer for the Kafka `orders` topic.
This service only consumes and logs messages; transformation (DPFMD-9),
storage (DPFMD-10) and retry/error handling (DPFMD-11) are out of scope.
"""

import json
import logging
import os

from kafka import KafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("dataingestion")

TOPIC = os.getenv("KAFKA_TOPIC", "orders")
BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
GROUP_ID = os.getenv("KAFKA_CONSUMER_GROUP", "dataingestion")
AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "latest")


def main() -> None:
    logger.info(
        "Starting Kafka consumer: broker=%s topic=%s group=%s auto_offset_reset=%s",
        BROKER, TOPIC, GROUP_ID, AUTO_OFFSET_RESET,
    )
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BROKER],
        group_id=GROUP_ID,
        auto_offset_reset=AUTO_OFFSET_RESET,
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
    )
    try:
        for message in consumer:
            logger.info(
                "Received order event: partition=%s offset=%s key=%s value=%s",
                message.partition,
                message.offset,
                message.key,
                json.dumps(message.value),
            )
    except KeyboardInterrupt:
        logger.info("Consumer interrupted, shutting down")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
