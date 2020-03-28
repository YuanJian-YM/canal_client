"""
init project, mysql log yaml
"""
import pymysql
import os
import yaml
import logging
from logging import handlers

PROJECT_PATH = os.path.dirname(os.path.realpath(__file__))
CONFIG_YAML_PATH = PROJECT_PATH.split('base')[0] + 'conf/config.yaml'
if not os.path.exists(CONFIG_YAML_PATH):
    raise FileNotFoundError(f"{CONFIG_YAML_PATH}:config file not exists")
YAML_CONFIG_DICT = yaml.safe_load(open(CONFIG_YAML_PATH, encoding='utf-8'))


class Log(object):
    """
    init logging
    """
    def __init__(self, log_path: str, level: str, is_print_to_screen: bool):
        """
        :param log_path:
        :param level:
        :param is_print_to_screen:
        """
        self.level = level
        self.log_path = log_path
        self.is_print_screen = is_print_to_screen
        log_folder_path = self.log_path.rsplit('/', 1)[0]
        if not os.path.exists(log_folder_path):
            os.makedirs(log_folder_path, exist_ok=True)
        self._init_handle()
        self.file_handler = self._set_log_level(self.file_handler)
        if self.is_print_screen:
            self.screen_handler = self._set_log_level(self.screen_handler)

    def __str__(self):
        return f'log_path:{self.log_path} level:{self.level} is_print_to_screen:{self.is_print_screen}'

    def __repr__(self):
        return f'log_path:{self.log_path} level:{self.level} is_print_to_screen:{self.is_print_screen}'

    def _init_handle(self):
        """
        init handle
        """
        self.logger = logging.getLogger("access")
        self.logger.setLevel(logging.DEBUG)
        self.file_handler = handlers.RotatingFileHandler(self.log_path, maxBytes=10240, backupCount=10)
        self.formatter = logging.Formatter(
            '[%(asctime)s] [%(filename)s:%(lineno)d] [%(levelname)s]\t%(message)s')
        self.file_handler.setFormatter(self.formatter)
        self.logger.addHandler(self.file_handler)
        if self.is_print_screen:
            self.screen_handler = logging.StreamHandler()
            self.screen_handler.setFormatter(self.formatter)
            self.screen_handler.setLevel(logging.DEBUG)
            self.logger.addHandler(self.screen_handler)

    def _set_log_level(self, handler):
        """
        set log level
        :param handler:
        :return:
        """
        if self.level == "debug":
            handler.setLevel(logging.DEBUG)
        elif self.level == "info":
            handler.setLevel(logging.INFO)
        elif self.level == "warn":
            handler.setLevel(logging.WARNING)
        elif self.level == "error":
            handler.setLevel(logging.ERROR)
        elif self.level == "critical":
            handler.setLevel(logging.CRITICAL)
        return handler

    def get_screen_handler(self):
        """
        get handle
        :return:
        """
        if self.is_print_screen:
            return self.screen_handler
        else:
            return None

    def get_file_handler(self):
        """
        :return:
        """
        return self.file_handler

    def get_logger(self):
        """
        :return:
        """
        return self.logger


LOG_HANDLE = Log(YAML_CONFIG_DICT["log_path"], YAML_CONFIG_DICT["log_level"],
                 YAML_CONFIG_DICT["log_is_print_to_screen"])
LOGGER = LOG_HANDLE.get_logger()


DB = pymysql.connect(
    host=YAML_CONFIG_DICT['mysql_config']['host'],
    port=YAML_CONFIG_DICT['mysql_config']['port'],
    user=YAML_CONFIG_DICT['mysql_config']['user'],
    password=YAML_CONFIG_DICT['mysql_config']['password'],
    charset=YAML_CONFIG_DICT['mysql_config']['charset']
)
