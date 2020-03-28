"""
kafka message class
"""

import json
from abc import ABC
from concurrent.futures import ThreadPoolExecutor
from base.subject import Subject
from kafka import KafkaConsumer
from kafka.structs import TopicPartition
from base.init import LOGGER


class KafkaSubject(Subject, ABC):
    """
    KafkaSubject
    """
    def __init__(self, kafka_address_ip_port: list, group_id: int, topic: list, thread_pool_count: int = 20):
        """
        :param kafka_address_ip_port: kafka服务器ip和端口，type is list
        :param group_id:kafka consumer group id
        :param topic:
        :param thread_pool_count: 线程池大小
        """
        self.kafka_address_ip_port = kafka_address_ip_port
        self.group_id = group_id
        self.topic = topic
        self.thread_pool_count = thread_pool_count
        self.executor = ThreadPoolExecutor(max_workers=thread_pool_count)

    def __str__(self):
        return f'address:{self.kafka_address_ip_port}, group_id:{self.group_id}, topic:{self.topic}'

    def __repr__(self):
        return f'address:{self.kafka_address_ip_port}, group_id:{self.group_id}, topic:{self.topic}'

    def get_message(self):
        """
        get message by kafka or canal_server
        """
        consumer = KafkaConsumer(group_id=str(self.group_id), bootstrap_servers=self.kafka_address_ip_port,
                                 auto_offset_reset='earliest')
        topic_partitions = []
        for topic in self.topic:
            topic_partitions.append(TopicPartition(topic=topic['topic_name'], partition=topic['partition']['number']))
        consumer.assign(topic_partitions)
        for topic in self.topic:
            if topic['partition']['offset'] is not None:
                consumer.seek(TopicPartition(topic=topic['topic_name'], partition=topic['partition']['number']),
                              topic['partition']['offset'])
        try:
            for message in consumer:
                result = json.loads(message.value)
                LOGGER.info(f'receive kafka message: {result}')
                self.executor.submit(self.notify, result)
        except:
            consumer.close()
