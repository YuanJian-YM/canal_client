"""
CanalSubject
"""
from abc import ABC
import time
from canal.client import Client
from concurrent.futures import ThreadPoolExecutor
from canal.protocol import EntryProtocol_pb2
from base.subject import Subject
from base.init import LOGGER


class CanalSubject(Subject, ABC):
    """
    CanalSubject
    """
    def __init__(self, canal_server_host: str, canal_port: int, canal_mysql_username: str, canal_mysql_password: str,
                 client_id: int, destination: str, sleep_time: int = 1, get_count: int = 100,
                 thread_pool_count: int = 20):
        self.client = Client()
        self.canal_server_host = canal_server_host
        self.canal_port = canal_port
        self.canal_mysql_username = canal_mysql_username
        self.canal_mysql_password = canal_mysql_password
        self.client_id = client_id
        self.destination = destination
        self.sleep_time = sleep_time
        self.get_count = get_count
        self.init_canal_client()
        self.executor = ThreadPoolExecutor(max_workers=thread_pool_count)

    def __str__(self):
        return f'canal_server_host:{self.canal_server_host} canal_port:{self.canal_port} canal_mysql_username:' \
               f'{self.canal_mysql_username} canal_mysql_password{self.canal_mysql_password} destination{self.destination}'

    def __repr__(self):
        return f'canal_server_host:{self.canal_server_host} canal_port:{self.canal_port} canal_mysql_username:' \
               f'{self.canal_mysql_username} canal_mysql_password{self.canal_mysql_password} destination{self.destination}'

    def init_canal_client(self):
        """
        init canal client
        :return:
        """
        self.client.connect(host=self.canal_server_host, port=self.canal_port)
        self.client.check_valid(username=self.canal_mysql_username.encode('utf-8'),
                                password=self.canal_mysql_password.encode('utf-8'))
        self.client.subscribe(client_id=str(self.client_id).encode('utf-8'),
                              destination=self.destination.encode('utf-8'), filter=b'.*\\..*')

    def get_message(self):
        """
        get message by kafka or canal_server
        """
        while True:
            try:
                message = self.client.get(100)
                entries = message['entries']
                for entry in entries:
                    entry_type = entry.entryType
                    if entry_type in [EntryProtocol_pb2.EntryType.TRANSACTIONBEGIN,
                                      EntryProtocol_pb2.EntryType.TRANSACTIONEND]:
                        continue
                    row_change = EntryProtocol_pb2.RowChange()
                    row_change.MergeFromString(entry.storeValue)
                    header = entry.header
                    database = header.schemaName
                    table = header.tableName
                    event_type = header.eventType
                    format_data = dict()
                    for row in row_change.rowDatas:
                        if event_type == EntryProtocol_pb2.EventType.DELETE:
                            for column in row.beforeColumns:
                                format_data[column.name] = column.value
                        elif event_type == EntryProtocol_pb2.EventType.INSERT:
                            for column in row.afterColumns:
                                format_data[column.name] = column.value
                        else:
                            format_data['before'] = dict()
                            format_data['after'] = dict()
                            for column in row.beforeColumns:
                                format_data['before'][column.name] = column.value
                            for column in row.afterColumns:
                                format_data['after'][column.name] = column.value
                        data = dict(
                            db=database,
                            table=table,
                            event_type=event_type,
                            data=format_data,
                        )
                        LOGGER.info(f"receive canal server message: {data}")
                        self.executor.submit(self.notify, data)
                time.sleep(self.sleep_time)
            except:
                self.client.disconnect()
