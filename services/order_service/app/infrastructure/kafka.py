from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import (
    KafkaTimeoutError,
    NotLeaderForPartitionError,
    TopicAlreadyExistsError,
    UnknownTopicOrPartitionError,
)


async def ensure_topics(bootstrap_servers: str, topics: list[str]) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        try:
            await admin.create_topics(
                [NewTopic(name=t, num_partitions=1, replication_factor=1) for t in topics]
            )
        except TopicAlreadyExistsError:
            pass
    finally:
        await admin.close()


class KafkaEventPublisher:

    def __init__(self, bootstrap_servers: str) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
        )

    async def start(self) -> None:
        await self.producer.start()

    async def stop(self) -> None:
        await self.producer.stop()

    async def publish(
        self,
        topic: str,
        value: bytes,
    ) -> None:
        try:
            await self.producer.send_and_wait(topic, value)
        except (
            NotLeaderForPartitionError,
            UnknownTopicOrPartitionError,
            KafkaTimeoutError,
        ):
            await ensure_topics(self.bootstrap_servers, [topic])
            await self.producer.client.force_metadata_update()
            await self.producer.send_and_wait(topic, value)