"""
canal 具体的观察者
"""
from abc import ABC
from base.observer import Observer
from kafka_client.kafka_observer import KafkaObserver
from base.subject import Subject
from base.init import LOGGER, DB


class CanalObserver(Observer, ABC):
    """
    CanalObserver
    """
    def __init__(self, subject: Subject):
        """
        add observer
        :param subject:
        """
        self.subject = subject
        self.subject.attach(self)

    def update(self, message: dict):
        """
        override
        :param message:
        :return:
        """
        sql = CanalObserver.parser_message_to_mysql(message)
        CanalObserver.exec_mysql(sql)

    @staticmethod
    def parser_message_to_mysql(data: dict) -> list:
        """
        把消息转换成mysql语句
        :param data:
        :return:
        """
        operator_type = data['event_type']
        database = data['db']
        table = data['table']
        content = data['data']
        mysql = []
        if operator_type == 1:
            # insert
            format_str = f'insert into {database}.{table}('
            mysql.append(KafkaObserver.parser_insert(content, format_str))
        elif operator_type == 2:
            # update
            mysql.append(CanalObserver.parser_update(database, table, content))
        elif operator_type == 3:
            # delete
            format_str = f'delete from {database}.{table} where '
            mysql.append(KafkaObserver.parser_delete(content, format_str))
        return mysql

    @staticmethod
    def exec_mysql(sql: list):
        """
        执行mysql语句
        :param sql:
        :return:
        """
        for exec_sql in sql:
            LOGGER.info(f"exec: {exec_sql}")
            cursor = DB.cursor()
            try:
                cursor.execute(exec_sql)
                DB.commit()
            except:
                LOGGER.error(f"exec: {exec_sql} failed")
            finally:
                cursor.close()

    @staticmethod
    def parser_update(database: str, table: str, content: dict) -> str:
        """
        解析update消息,生成mysql语句
        :param database:
        :param table:
        :param content:
        :return:
        """
        format_str = f"update {database}.{table} set "
        update_before = content['before']
        update_after = content['after']
        different = {}
        for key, value in update_after.items():
            if value != update_before[key]:
                different[key] = value
        for key, value in different.items():
            format_str = format_str + key + "='" + value + "', "
        format_str = format_str.rsplit(',', 1)[0] + ' where '
        for key, value in update_before.items():
            format_str = format_str + key + "='" + value + "' and "
        return format_str.rsplit('and', 1)[0] + ';'
