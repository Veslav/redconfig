""" Driver Interface """
from abc import ABCMeta, abstractmethod


class IDriver(metaclass=ABCMeta):
    """ Driver Interface """

    @abstractmethod
    def __init__(self, connection_string: str = None, **kwargs):
        pass

    @abstractmethod
    def set(self, path: str, value: str) -> bool:
        """ Set Value """
        pass

    @abstractmethod
    def set_many(self, path_value: dict) -> bool:
        """ Set Many Values """
        pass

    @abstractmethod
    def get(self, path: str) -> str:
        """ Get Value """
        pass

    @abstractmethod
    def get_many(self, path: str, not_path: str = '') -> dict or None:
        """ Get All Values """
        pass

    @abstractmethod
    def keys(self, path: str) -> list:
        """ List Keys """
        pass

    @abstractmethod
    def delete(self, path: str) -> list:
        """ Delete Key """
        pass

    @abstractmethod
    def delete_many(self, paths: list) -> bool:
        """ Delete many Keys """
        pass

    @abstractmethod
    def close(self):
        """ Close Storage """
        pass
