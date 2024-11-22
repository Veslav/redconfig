""" Redis Driver """
import re

import redis

from .idriver import IDriver


class RedisDriver(IDriver):
    """ Redis Driver """

    def __init__(self, connection_string: str = None, **kwargs):
        """
        Создаёт объект для доступа к данным
        :param connection_string: 'user:password@localhost:6379/0'
        """
        if connection_string and connection_string.startswith('redis://'):
            user, password, host, port, dbs = parse_connection(connection_string)
        else:
            raise ValueError('connection_string must starts with "redis://"')
        self.redis = redis.StrictRedis(host=host,
                                       port=port,
                                       db=dbs,
                                       username=user,
                                       password=password,
                                       decode_responses=True)

    def set(self, path: str, value: str) -> bool:
        return self.redis.set(path, value)

    def set_many(self, path_value: dict) -> bool:
        return self.redis.mset(path_value)

    def get(self, path: str) -> str:
        value = self.redis.get(path)
        return value

    def get_many(self, path: str, not_path: str = '') -> dict or None:
        keys = self.redis.keys(path)
        if not keys:
            return None
        values = self.redis.mget(keys)
        res = dict(zip(keys, values))
        return res

    def keys(self, path: str) -> list:
        keys = self.redis.keys(path)
        return keys

    def delete(self, path: str) -> list:
        """ Delete Key """
        keys = self.redis.keys(path)
        if not keys:
            return []
        res = []
        for key in keys:
            if self.redis.delete(key) == 1:
                res.append(key)
        return res

    def delete_many(self, paths: list) -> list:
        """ Delete many Keys """
        res = []
        for path in paths:
            res.extend(self.delete(path))
        return res

    def close(self):
        return self.redis.close()


def parse_connection(connection_string):
    """ Parse connection string """
    items = re.findall(r'redis://(.*):(.*)@(.+):(.+)/(.+)', connection_string)
    if not items:
        raise ValueError
    return items[0]
