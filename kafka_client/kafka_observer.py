"""
kafka base class
"""
from abc import ABC
from base.observer import Observer
from base.subject import Subject
from base.init import LOGGER, DB


class KafkaObserver(Observer, ABC):
    """
    kafka 观察者
    """
    def __init__(self, subject: Subject):
        self.subject = subject
        self.subject.attach(self)

    def update(self, message: dict):
        """
        定义具体的行为
        """
        sql = KafkaObserver.parser_message_to_mysql(message)
        KafkaObserver.exec_mysql(sql)

    @staticmethod
    def parser_message_to_mysql(message: dict) -> list:
        """
        parser message to mysql
        """
        mysql = []
        operator_type = message.get("type")
        database = message.get("database")
        table = message.get("table")
        data = message.get("data")
        if operator_type == "INSERT":
            for row in data:
                format_str = f'insert into {database}.{table}('
                mysql.append(KafkaObserver.parser_insert(row, format_str))
        elif operator_type == "UPDATE":
            old = message.get("old")
            mysql.extend(KafkaObserver._parser_update(database, table, data, old))
        elif operator_type == "DELETE":
            for row in data:
                format_str = f'delete from {database}.{table} where '
                mysql.append(KafkaObserver.parser_delete(row, format_str))
        return mysql

    @staticmethod
    def exec_mysql(sql: list):
        """
        exec mysql
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
    def parser_delete(row: dict, format_str: str) -> str:
        """
        解析delete 消息，生成mysql语句
        :param row:
        :param format_str:
        :return:
        """
        for key, value in row.items():
            format_str = format_str + key + "='" + value + "' and "
        return format_str.rsplit('and', 1)[0] + ';'

    @staticmethod
    def parser_insert(row: dict, format_str: str) -> str:
        """
        解析insert 消息，生成mysql语句
        :param row:
        :param format_str:
        :return:
        """
        column_values = []
        column_keys = []
        for key, value in row.items():
            column_keys.append(key)
            column_values.append(value)
        for i, key in enumerate(column_keys):
            if i == (len(column_keys) - 1):
                format_str = format_str + key + ') '
            else:
                format_str = format_str + key + ', '
        for i, value in enumerate(column_values):
            if i == 0:
                format_str = format_str + "values('" + value + "', "
            elif i == (len(column_values) - 1):
                format_str = format_str + "'" + value + "');"
            else:
                format_str = format_str + "'" + value + "', "
        return format_str

    @staticmethod
    def _parser_update(database: str, table: str, data: list, old: list) -> list:
        """
        解析update消息，生成mysql语句
        :param database:
        :param table:
        :param data:
        :param old:
        :return:
        """
        sql = []
        if len(data) != len(old):
            return sql
        for i in range(len(data)):
            format_str = f"update {database}.{table} set "
            for key, value in old[i].items():
                format_str = format_str + key + "='" + data[i][key] + "',"
                data[i][key] = value
            format_str = format_str.rsplit(',', 1)[0] + ' where '
            for key, value in data[i].items():
                format_str = format_str + key + "='" + value + "' and "
            sql.append(format_str.rsplit('and', 1)[0] + ';')
        return sql
