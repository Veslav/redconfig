""" Hazelcast Driver """
import os
import re
from pathlib import Path

from .idriver import IDriver


class FileSystemDriver(IDriver):
    """Redis Driver"""

    def __init__(self, connection_string: str = None, **kwargs):
        """
        Создаёт объект для доступа к данным
        :param connection_string: 'file://configmap'
        """
        if connection_string and connection_string.startswith("file://"):
            self.root_path = Path(parse_connection(connection_string)).absolute()
        else:
            raise ValueError('connection_string must starts with "file://"')
        self.exclude = set(kwargs.get("exclude") or [])

    def set(self, path: str, value: str) -> bool:
        try:
            _path = self.get_abs_path(path)
            _name = _path.stem + ".yaml"  # TODO: формат файла может быть json
            file = Path(_path, _name)
            if not _path.exists():
                _path.mkdir(parents=True, exist_ok=True)
            file.write_text(value)
            return True
        except Exception as err:
            print(err)
            return False

    def set_many(self, path_value: dict) -> bool:
        for path, value in path_value.items():
            self.set(path, value)
        return True

    def get(self, path: str) -> str | None:
        try:
            _path = self.get_abs_path(path)
            values = []
            for file in _path.glob("*.yaml"):
                if file.stem in self.exclude:
                    continue
                values.append(file.read_text())
            return "\n".join(values) if values else None
        except Exception as err:
            print(err)
            return None

    def get_many(self, path: str, not_path: str = "") -> dict or None:
        res = {}
        try:
            abs_path = self.get_abs_path(path)
            if abs_path.is_dir():
                for key in abs_path.glob("*"):
                    if key.is_dir():
                        p = self.to_key_path(key)
                        m = self.get_many(p)
                        res.update(m)
                    if key.is_file():
                        p = self.to_key_path(key.parent)
                        value = key.read_text()
                        res[p] = value
                return res
            else:
                startswith = abs_path.stem
                abs_path = abs_path.parent
                for key in abs_path.glob(startswith):
                    if key.is_dir():
                        p = self.to_key_path(key)
                        m = self.get_many(p)
                        res.update(m)
                    if key.is_file():
                        p = self.to_key_path(key.parent)
                        value = key.read_text()
                        res[p] = value
                return res
        except Exception as err:
            print(err)
            return None

    def keys(self, path: str) -> list:
        res = []
        abs_path = self.get_abs_path(path)
        if abs_path.is_dir():
            res.append(self.to_key_path(abs_path))
        else:
            startswith = abs_path.stem
            abs_path = abs_path.parent
            for key in abs_path.glob(startswith):
                if key.is_dir():
                    res.append(self.to_key_path(key))
        return res

    def delete(self, path: str) -> list:
        """Delete Key"""
        res = []
        try:
            abs_path = self.get_abs_path(path)
            if abs_path.is_dir():
                for key in abs_path.glob("*"):
                    if key.is_dir():
                        res.extend(self.delete(self.to_key_path(key)))
                    if key.is_file():
                        os.remove(key)
                res.append(self.to_key_path(abs_path))
                abs_path.rmdir()
            else:
                startswith = abs_path.stem
                abs_path = abs_path.parent
                for key in abs_path.glob(startswith):
                    if key.is_dir():
                        res.extend(self.delete(self.to_key_path(key)))
                    if key.is_file():
                        os.remove(key)
        except Exception as err:
            print(err)
        return res

    def delete_many(self, paths: list) -> list:
        """Delete many Keys"""
        res = []
        for path in paths:
            res.extend(self.delete(path))
        return res

    def close(self):
        return

    def get_abs_path(self, path):
        return Path(self.root_path, path.replace(":", "/").replace(".", "/"))

    def to_key_path(self, key):
        return ":".join(key.relative_to(self.root_path).parts)


def parse_connection(connection_string):
    """Parse connection string"""
    items = re.findall(r"file://(.+)", connection_string)
    if not items:
        raise ValueError
    return items[0]
