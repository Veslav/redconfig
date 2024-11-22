""" Helpers """
import os
import pathlib
from datetime import datetime

from redconfig import ConfigManager


def set_configs_from_files(
    rc: ConfigManager,
    ospath: str,
    root: str = "",
    ext=".yaml",
    file_as_path=False,
    exclude=tuple(),
    with_attrs=False,
):
    """
    Сохраняет конфиги из файлов в Хранилище
    """
    bwd = os.getcwd()
    os.chdir(ospath)
    try:
        params = {}
        for pwd, _, names, _ in os.fwalk():
            _pwd = pathlib.Path(pwd)
            if set(_pwd.parts) & set(exclude):
                continue
            pwd = str(_pwd)
            print("setup", pwd)
            for name in sorted(names):
                if not name.endswith(ext):
                    continue
                file_path = pathlib.Path(pwd, name)
                if file_path.stem in exclude:
                    continue
                # print(file_path)
                with open(file_path) as file:
                    new = file.read()
                    if new and new[-1] != "\n":
                        new += "\n"
                path = pwd.replace("/", ":")
                if file_path.stem.startswith(":"):
                    path = root + ":" + path + file_path.stem
                else:
                    if path == ".":
                        path = root + (":" + file_path.stem if file_as_path else "")
                    elif root:
                        path = root + ":" + (file_path.stem if file_as_path else path)
                if cfg := params.get(path):
                    cfg["value"] += new
                else:
                    attrs = None
                    if rc.with_attrs:
                        old, old_attrs = rc.get_one(path, source=True)
                        if old == new:
                            continue
                        attrs = dict(
                            rev=int(old_attrs["rev"]) + 1 if old_attrs else 1,
                            time=datetime.utcnow().isoformat(),
                            user="anonymous",
                        )
                    cfg = dict(value=new, attrs=attrs)
                if cfg["value"]:
                    params[path] = cfg
        if params:
            print("set many to table...")
            rc.set_many(params)
    finally:
        os.chdir(bwd)


def set_from_file(rc: ConfigManager, path: str, file_name: str):
    """
    Записать значение из файла
    :param rc:
    :param path:
    :param file_name:
    :return:
    """
    with open(file_name) as file:
        rc.set(path, file.read())


def delete(rc: ConfigManager, path: str):
    """
    Удаляет ключи по маске
    :param rc:
    :param path:
    :return:
    """
    for _key in rc.keys(path):
        rc.delete(_key)
