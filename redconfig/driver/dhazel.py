""" Hazelcast Driver """
import re

import hazelcast
from hazelcast.config import Config
from hazelcast.predicate import like

from .idriver import IDriver


class HazelcastDriver(IDriver):
    """ Redis Driver """

    def __init__(self, connection_string: str = None, **kwargs):
        """
        Создаёт объект для доступа к данным
        :param connection_string: 'hazelcast://user:password@localhost/configmap'
        """
        if connection_string and connection_string.startswith('hazelcast://'):
            user, password, host, port, dbs = parse_connection(connection_string)
        else:
            raise ValueError('connection_string must starts with "hazelcast://"')
        config = Config()
        config.cluster_members = [host]
        config.connection_timeout = 5.0
        # config.ssl_enabled = True
        config.creds_password = password
        config.creds_username = user
        self.client = hazelcast.HazelcastClient()
        self.map = self.client.get_map(dbs).blocking()

    def set(self, path: str, value: str) -> bool:
        self.map.set(path, value)
        return True

    def set_many(self, path_value: dict) -> bool:
        self.map.put_all(path_value)
        return True

    def get(self, path: str) -> str:
        value = self.map.get(path)
        return value

    def get_many(self, path: str, not_path: str = '') -> dict or None:
        predicate = like('__key', path.replace('*', '%'))
        res = {key: val for key, val in self.map.entry_set(predicate)}
        return res

    def keys(self, path: str) -> list:
        predicate = like('__key', path.replace('*', '%'))
        keys = [key for key in self.map.key_set(predicate)]
        return keys

    def delete(self, path: str) -> list:
        """ Delete Key """
        keys = self.keys(path)
        res = []
        for key in keys:
            self.map.delete(key)
            res.append(key)
        return res

    def delete_many(self, paths: list) -> list:
        """ Delete many Keys """
        res = []
        for path in paths:
            res.extend(self.delete(path))
        return res

    def close(self):
        return self.client.shutdown()


def parse_connection(connection_string):
    """ Parse connection string """
    items = re.findall(r'hazelcast://(.*):(.*)@(.+):?(.*)/(.+)', connection_string)
    if not items:
        raise ValueError
    return items[0]
