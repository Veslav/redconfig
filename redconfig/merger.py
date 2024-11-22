""" Слияние произвольных объектов """
import itertools
from typing import Any, Callable

__merge_list = True


def set_merge_list(val: bool):
    global __merge_list
    __merge_list = val


def merge(a: Any, b: Any, replace: Callable, ext=False) -> Any:
    """Сливает объекты a и b"""
    if isinstance(b, list) or isinstance(a, list):
        return merge_list(a, b, replace)
    if isinstance(b, dict) or isinstance(a, dict):
        return merge_dict(a, b, replace)
    if ext and b is not None and a is not None:
        # берем только значения, ключи не нужны
        return [replace(a)[0], replace(b)[0]]
    equal, _ = replace(b if b is not None else a)
    return equal


def merge_list(a: list, b: list, replace: Callable) -> list:
    """Сливает списки a и b"""
    a_list = a or []
    _equal = []
    if __merge_list:
        # соединяем списки друг за другом, если значения простые типы
        b_list = b or []
        iter_list = itertools.chain(a_list, b_list)
        for x_val in iter_list:
            if not isinstance(x_val, (str, int, float)):
                break
            _val = merge(None, x_val, replace, ext=True)
            _equal.append(_val)
        else:
            return _equal or a_list
    else:
        # или совместимость с версией 0.10
        if b is None:
            return a_list
        b_list = b or []
        for x_val in b_list:
            if not isinstance(x_val, (str, int, float)):
                break
            _val = merge(None, x_val, replace, ext=True)
            _equal.append(_val)
        else:
            return _equal
    # если значение объекты, мержим парами
    _equal = []
    for a_val, b_val in itertools.zip_longest(a_list, b_list):
        _val = merge(a_val, b_val, replace, ext=True)
        if isinstance(_val, list):
            _equal.extend(_val)
        else:
            _equal.append(_val)
    return _equal


def merge_dict(a: dict, b: dict, replace: Callable) -> dict:
    """Сливает словари a и b"""
    try:
        _equal = {}
        for key, a_val, b_val in _zip_dicts(a, b):
            _b_val, _key = replace(b_val, key)
            _val = merge(a_val, _b_val, replace)
            _equal[_key] = merge(_equal.get(_key), _val, replace)
        return _equal
    except Exception as err:
        raise err


def _zip_dicts(a: dict, b: dict) -> (str, Any, Any):
    """Соединение значений из двух словарей"""
    a_dict = a or {}
    b_dict = b or {}
    fields = sorted(frozenset(a_dict.keys()) | frozenset(b_dict.keys()))
    for key in fields:
        yield key, a_dict.get(key), b_dict.get(key)
