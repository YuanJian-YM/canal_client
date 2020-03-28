"""
main.py
"""
from kafka_client.kafka_subject import KafkaSubject
from kafka_client.kafka_observer import KafkaObserver
from subscribe_client.canal_observer import CanalObserver
from subscribe_client.canal_subject import CanalSubject
from base.init import YAML_CONFIG_DICT, LOGGER


def run():
    """
    run
    """
    if YAML_CONFIG_DICT['use_kafka_not_use_subscribe']:
        kafka_subject = KafkaSubject(YAML_CONFIG_DICT['kafka_config']['address'],
                                     YAML_CONFIG_DICT['kafka_config']['group_id'],
                                     YAML_CONFIG_DICT['kafka_config']['topic'],
                                     YAML_CONFIG_DICT['kafka_config']['thread_pool_count'])
        kafka_base = KafkaObserver(kafka_subject)
        LOGGER.info(f"connect kafka: {YAML_CONFIG_DICT['kafka_config']['address']}, 开始处理消息！")
        kafka_subject.get_message()
    else:
        canal_subject = CanalSubject(YAML_CONFIG_DICT['subscribe_config']['host'],
                                     YAML_CONFIG_DICT['subscribe_config']['port'],
                                     YAML_CONFIG_DICT['subscribe_config']['username'],
                                     YAML_CONFIG_DICT['subscribe_config']['password'],
                                     YAML_CONFIG_DICT['subscribe_config']['client_id'],
                                     YAML_CONFIG_DICT['subscribe_config']['destination'],
                                     YAML_CONFIG_DICT['subscribe_config']['sleep_time'],
                                     YAML_CONFIG_DICT['subscribe_config']['get_count'],
                                     YAML_CONFIG_DICT['subscribe_config']['thread_pool_count'])
        canal_observer = CanalObserver(canal_subject)
        LOGGER.info(
            f"connect canal server:{YAML_CONFIG_DICT['subscribe_config']['host']}:{YAML_CONFIG_DICT['subscribe_config']['port']} 开始处理消息！")
        canal_subject.get_message()


if __name__ == "__main__":
    run()
