""" Config Manager """
import collections.abc
import itertools
import logging
import re
from copy import copy
from datetime import datetime
from typing import Any, Collection

import yaml

from . import merger
from .driver import RedisDriver, SQLDriver, IDriver, FileSystemDriver
from .driver.dhazel import HazelcastDriver


class ConfigManager1:
    """Config Manager"""

    PATTERN = re.compile(r"\$\$:?([\w.:-]+):?\$?\$?")
    SUB_PATTERN = re.compile(r"(\$\$:?[\w.:-]+:?\$?\$?)")
    KEY_PATTERNS = [re.compile(r"<<([\w\S]+)"), re.compile(r"([\w\S]+):\$\$")]
    ROOT = "rc"

    def __init__(
        self,
        connection_string: str = None,
        table_name: str = "redconfig",
        schema: str = "configmap",
        merge_list: bool = True,
        exclude: Collection = None,
    ):
        """
        Создаёт объект RedConfig
        :param connection_string: 'driver://user:password@localhost:6379/0'
        :param table_name: 'redconfig'
        :param schema: 'configmap'
        """
        # не заменять строки с датами на тип date или datetime
        yaml.constructor.SafeConstructor.yaml_constructors[
            "tag:yaml.org,2002:timestamp"
        ] = yaml.constructor.SafeConstructor.yaml_constructors["tag:yaml.org,2002:str"]
        self.cache = {}
        self.merge_list = merge_list
        merger.set_merge_list(self.merge_list)
        if connection_string.startswith("postgresql://"):
            self.driver: IDriver = SQLDriver(
                connection_string,
                table_name=table_name,
                schema=schema,
                exclude=exclude,
            )
            return
        if connection_string.startswith("file://"):
            self.driver: IDriver = FileSystemDriver(connection_string, exclude=exclude)
            return
        if connection_string.startswith("redis://"):
            self.driver: IDriver = RedisDriver(connection_string, exclude=exclude)
            return
        if connection_string.startswith("hazelcast://"):
            self.driver: IDriver = HazelcastDriver(connection_string, exclude=exclude)
            return
        raise ValueError(
            'connection_string must starts with "postgresql://" or "redis://" or "hazelcast://"'
        )

    def close(self):
        """
        Закрывает соединение с базой данных
        :return:
        """
        self.cache.clear()
        self.driver.close()

    def delete(self, path: str) -> bool:
        """
        Удалить одно значение
        :param path:
        :return:
        """
        keys = self.driver.delete(f"{self.ROOT}:{path}")
        for key in keys:
            self.cache.pop(key, None)
        return True if keys else False

    def keys(self, path: str) -> list:
        """
        Список ключей по маске
        :param path:
        :return:
        """
        dbkeys = self.driver.keys(f"{self.ROOT}:{path}")
        keys = [":".join(key.split(":")[1:]) for key in dbkeys]
        return keys

    def set(self, path: str, value: Any) -> bool:
        """
        Записать одно значение
        :param path:
        :param value:
        :return:
        """
        key = f"{self.ROOT}:{path}"
        return self.driver.set(key, value)

    def set_many(self, path_value: dict) -> bool:
        """
        Записать несколько значений
        :param path_value: dict(path:value)
        :return:
        """
        _path_value = {
            f"{self.ROOT}:{path}": value for path, value in path_value.items()
        }
        return self.driver.set_many(_path_value)

    def load_cache(self, path: str = "*", not_path: str = "") -> dict:
        """
        Загружает все ключи в кэш
        :param path: Строка с разделителями ':'
        :param not_path:
        """
        try:
            _path_layer = self.driver.get_many(
                f"{self.ROOT}:{path}", f"{self.ROOT}:{not_path}"
            )
            if not _path_layer:
                return self.cache
            for _path, _layer in _path_layer.items():
                self.cache[_path] = _layer
            return self.cache
        except Exception as err:
            raise err

    def get_one(self, path: str, source=False) -> dict or str:
        """
        Собирает указанный конфиг из иерархии
        :param path: Строка с разделителями ':'
        :param source: Выгрузить оригинальный текст или словать или
        """
        try:
            key = f"{self.ROOT}:{path}"
            if (layer := self.cache.get(key)) is None:
                layer = self.driver.get(key)
                self.cache[key] = layer
            if source:
                return layer
            return yaml.safe_load(layer) if layer is not None else None
        except Exception as err:
            raise err

    def get(self, path: str, recurse=True) -> dict or None:
        """
        Собирает указанный конфиг из иерархии
        :param path: Строка с разделителями ':'
        :param recurse: Собрать конфиг рекурсивно
        """
        if not recurse:
            return self.get_one(path)
        path_list = path.split(":")
        end = len(path_list) + 1
        configs = [":".join(path_list[:i]) for i in range(1, end)]
        config = None
        for key in configs:
            for sub in self.sub_path(key):
                layer = self.get_one(sub)
                if layer:
                    config = merger.merge(config, layer, self.replace_placeholder)
        return config

    @staticmethod
    def sub_path(path: str) -> list:
        """
        Создает комбинации путей созданных через +
        :param path:
        :return:
        """
        if not path.find("+"):
            return [path]
        keys = [[]]
        paths = path.split(":")
        for key in paths:
            subs = key.split("+")
            if len(subs) == 1:
                for k in range(len(keys)):
                    keys[k].append(key)
            else:
                for k in range(len(keys)):
                    # скопировать путь накопленный к этому времени
                    src = copy(keys[k])
                    for s, mod in enumerate(subs):
                        if len(keys) < s + 1:
                            keys.append(copy(src))
                        keys[s].append(mod)
                    for mod in list(itertools.permutations(subs)):
                        keys.append(copy(src))
                        keys[len(keys) - 1].extend(mod)
        result = [":".join(k) for k in keys]
        return result

    def delete_many(self, paths: list) -> bool:
        keys = [f"{self.ROOT}:{path}" for path in paths]
        keys = self.driver.delete_many(keys)
        return True if keys else False

    def get_tree(self, path: str) -> dict:
        """
        Собирает указанный конфиг из иерархии
        :param path: Строка с разделителями ':'
        """

        path = self.ROOT + ":" + path.replace("*", ".*")

        def flt_path(x):
            if re.search(path, x):
                return True
            return False

        tree = {}
        for key in filter(flt_path, self.cache.keys()):
            item = {}
            _path = key.split(":")
            v = item
            for k in _path:
                v = v.setdefault(k, {})
            tree = self.update_tree(tree, item)
        return tree.get(self.ROOT, {})

    def update_tree(self, d, u):
        for k, v in u.items():
            if isinstance(v, collections.abc.Mapping):
                d[k] = self.update_tree(d.get(k, {}), v)
            else:
                d[k] = v
        return d

    def replace_placeholder(self, value: str, key=None) -> Any:
        """
        Получить объект описанный в placeholder
        :param value: $$<Path_for_value>
        :param key: <<key
        """
        try:
            if not isinstance(value, str):
                return value, key
            if key:
                key, value = self.replace_key(key, value)
            if not isinstance(value, str):
                return value, key
            if not (allplace := re.findall(self.PATTERN, value)):
                return value, key
            result = value
            for place in allplace:
                if (value := self.get_placeholder(place)) is None:
                    continue
                if isinstance(value, str):
                    result = re.sub(self.SUB_PATTERN, value, result)
                else:
                    result = value
            return result, key
        except Exception as err:
            logging.exception(err)
            logging.error(repr(err))
            return value, key

    def replace_key(self, key, value) -> (str, Any):
        """
        Проверка ключа на потерн и замена плэйсхолдера на значение
        :param key:
        :param value:
        :return: key, value
        """
        for p in self.KEY_PATTERNS:
            # Проверяем все шаблоны для ключей, используем первый найденный
            if keys := re.findall(p, key):
                key = keys[0]
                value = self.get_key_placeholder(value)
                break
        return key, value

    def get_key_placeholder(self, placeholder: str) -> Any:
        """
        Получить объект описанный в  placeholder
        :param placeholder: $$<red_key>[:<red_key>][.<dict_key>][.<dict_key>]
        """
        if allplace := re.findall(self.PATTERN, placeholder):
            return self.get_placeholder(allplace[0])
        else:
            return self.get_placeholder(placeholder)

    def get_placeholder(self, placeholder: str) -> Any:
        """
        Получить объект описанный в  placeholder
        :param placeholder: <red_key>[:<red_key>][.<dict_key>][.<dict_key>]
        """
        path, *keys = placeholder.rstrip(":").lstrip(":").split(".")
        holder = self.get(path)
        if not holder:
            raise ValueError(placeholder)
        for i, k in enumerate(keys, 1):
            val = holder.get(k)
            if i == len(keys):
                return val
            if isinstance(val, dict):
                holder = val
            else:
                raise ValueError(placeholder)
        return holder


class ConfigManager(ConfigManager1):
    """Config Manager"""

    def __init__(
        self,
        connection_string: str = None,
        table_name: str = "redconfig",
        schema: str = "configmap",
        merge_list: bool = True,
        with_attrs: bool = False,
        exclude: Collection = None,
    ):
        """
        Создаёт объект RedConfig
        :param connection_string: 'driver://user:password@localhost:6379/0'
        :param table_name: 'redconfig'
        :param schema: 'configmap'
        """
        super().__init__(connection_string, table_name, schema, merge_list, exclude)
        if connection_string.startswith("file:"):
            self.with_attrs = False
            self.suffix = ""
        else:
            self.with_attrs = with_attrs
            self.suffix = "#*" if self.with_attrs else ""

    @staticmethod
    def _make_key(path: str, attrs: dict = None) -> str:
        key = path
        if not attrs:
            return key
        for k, v in attrs.items():
            key += f"#{k}={v}"
        return key

    @staticmethod
    def _split_key(key: str) -> (str, dict):
        part = key.split("#")
        path = part[0]
        attrs = {}
        for a in part[1:]:
            k, v = a.split("=")
            attrs[k] = v
        return path, attrs if attrs else None

    def load_cache(self, path: str = "*", not_path: str = "") -> dict:
        """
        Загружает все ключи в кэш
        :param path: Строка с разделителями ':'
        :param not_path:
        """
        try:
            _path_layer = self.driver.get_many(
                f"{self.ROOT}:{path}", f"{self.ROOT}:{not_path}"
            )
            if not _path_layer:
                return self.cache
            for _key, _layer in _path_layer.items():
                _path, _attrs = self._split_key(_key)
                self.cache[_path] = (_layer, _attrs)
            return self.cache
        except Exception as err:
            raise err

    def set(self, path: str, value: Any, attrs: dict = None) -> bool:
        """
        Записать одно значение
        :param path:
        :param value:
        :param attrs:
        :return:
        """
        old, old_attrs = self.get_one(path, source=True)
        if old == value:
            return True
        elif old is not None:
            old_key = self.ROOT + ":" + self._make_key(path, old_attrs)
            if not self.driver.delete(old_key):
                return False
        if self.with_attrs:
            if not attrs:
                attrs = dict(
                    rev=1, time=datetime.utcnow().isoformat(), user="anonymous"
                )
            key = self._make_key(path, attrs)
        else:
            key = self._make_key(path)
        return super().set(key, value)

    def set_many(self, path_value: dict) -> bool:
        """
        Записать несколько значений
        :param path_value: dict(path:dict(value:str,attrs:dict))
        :return:
        """
        key_value = {}
        old_keys = []
        for k, v in path_value.items():
            old_val, old_attrs = self.get_one(k, source=True)
            if old_val != v["value"]:
                if old_attrs:
                    v["attrs"]["rev"] = int(old_attrs["rev"]) + 1
                    old_keys.append(self._make_key(k, old_attrs))
                key = self._make_key(k, v["attrs"])
                key_value[key] = v["value"]
        res = super().set_many(key_value)
        super().delete_many(old_keys)
        return res

    def get_one(self, path: str, source=False) -> dict or str:
        """
        Собирает указанный конфиг из иерархии
        :param path: Строка с разделителями ':'
        :param source: Выгрузить оригинальный текст или словарь
        """
        # logging.debug('get_one %s', path)
        try:
            find_path = self.ROOT + ":" + self._make_key(path)
            layer, attrs = self.cache.get(find_path, (None, None))
            if layer is None:
                for key in self.driver.keys(find_path + self.suffix):
                    short_path, attrs = self._split_key(key)
                    layer = self.driver.get(key)
                    self.cache[self.ROOT + ":" + path] = (layer, attrs)
                    break
            if source:
                return layer, attrs
            return yaml.safe_load(layer) if layer else {}, attrs
        except Exception as err:
            raise err

    def get(self, path: str, recurse=True) -> dict:
        """
        Собирает указанный конфиг из иерархии
        :param path: Строка с разделителями ':'
        :param recurse: Собрать конфиг рекурсивно
        """
        if not recurse:
            layer, attrs = self.get_one(path)
            return layer
        path_list = path.split(":")
        end = len(path_list) + 1
        configs = [":".join(path_list[:i]) for i in range(1, end)]
        config = None
        for key in configs:
            for sub in self.sub_path(key):
                layer, attrs = self.get_one(sub)
                if layer:
                    config = merger.merge(config, layer, self.replace_placeholder)
        return config

    def keys(self, path: str) -> list:
        """
        Список ключей по маске
        :param path:
        :return:
        """
        keys = super().keys(path + self.suffix)
        return keys

    def delete(self, path: str) -> bool:
        """
        Удалить одно значение
        :param path:
        :return:
        """
        keys = self.driver.delete(f"{self.ROOT}:{path}")
        for key in keys:
            _path, _attrs = self._split_key(key)
            self.cache.pop(_path, None)
        return True if keys else False

    # def get_placeholder(self, placeholder: str) -> Any:
    #     """
    #     Получить объект описанный в  placeholder
    #     :param placeholder: <red_key>[:<red_key>][.<dict_key>][.<dict_key>]
    #     """
    #     path, *keys = placeholder.rstrip(':').lstrip(':').split('.')
    #     holder = self.get(path)
    #     if not holder:
    #         raise ValueError(placeholder)
    #     for i, k in enumerate(keys, 1):
    #         val = holder.get(k)
    #         if i == len(keys):
    #             return val
    #         if isinstance(val, dict):
    #             holder = val
    #         else:
    #             raise ValueError(placeholder)
    #     return holder
