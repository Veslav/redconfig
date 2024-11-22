""" SQL Driver"""

import sqlalchemy
from sqlalchemy import Column, String, select, bindparam, delete, Table, MetaData, and_
from sqlalchemy.dialects.postgresql import insert

from .idriver import IDriver


class SQLDriver(IDriver):
    """ SQL Driver"""

    def __init__(self, connection_string: str, *,
                 connect_args: str = None,
                 table_name: str = 'redconfig',
                 schema: str = 'configmap',
                 **kwargs):
        engine = sqlalchemy.create_engine(
            connection_string,
            connect_args=connect_args or {},
        )
        self.engine = engine
        meta = MetaData()
        table = Table(table_name,
                      meta,
                      Column('key', String, primary_key=True),
                      Column('value', String),
                      )
        table.create(engine, checkfirst=True)
        self.select_stmt = select(table.c.key, table.c.value) \
            .where(and_(table.c.key.like(bindparam('path')),
                        table.c.key.not_like(bindparam('not_path'))))
        self.select_key_stmt = select(table.c.key) \
            .where(table.c.key.like(bindparam('path')))
        self.select_value_stmt = select(table.c.value) \
            .where(table.c.key.like(bindparam('path')))
        self.delete_stmt = delete(table) \
            .where(table.c.key.like(bindparam('path'))).returning(table.c.key)
        self.upsert_stmt = insert(table).values(key=bindparam('path'), value=bindparam('value')) \
            .on_conflict_do_update(index_elements=[table.c.key], set_=dict(value=bindparam('value')))

    def set(self, path: str, value: str) -> bool:
        try:
            with self.engine.connect() as conn:
                res = conn.execute(self.upsert_stmt, dict(path=path, value=value))
                conn.commit()
                if res.rowcount:
                    return True
                return False
        except Exception as err:
            raise err

    def set_many(self, path_value: dict) -> bool:
        if not path_value:
            return False
        try:
            params = [dict(path=path, value=value) for path, value in path_value.items()]
            with self.engine.connect() as conn:
                for par in params:
                    res = conn.execute(self.upsert_stmt, par)
                conn.commit()
                if res.rowcount:
                    return True
                return False
        except Exception as err:
            raise err

    def get(self, path: str) -> str:
        try:
            with self.engine.connect() as conn:
                value = conn.scalar(self.select_value_stmt, dict(path=path.replace('*', '%')))
            return value
        except Exception as err:
            raise err

    def get_many(self, path: str, not_path: str = '') -> dict or None:
        try:
            with self.engine.connect() as conn:
                res = conn.execute(self.select_stmt, dict(path=path.replace('*', '%'),
                                                          not_path=not_path.replace('*', '%'),
                                                          )).all()
            if not res:
                return None
            return {row.key: row.value for row in res}
        except Exception as err:
            raise err

    def keys(self, path: str) -> list:
        try:
            with self.engine.connect() as conn:
                keys = list(conn.scalars(self.select_key_stmt, dict(path=path.replace('*', '%'))))
            return keys
        except Exception as err:
            raise err

    def delete(self, path: str) -> list:
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(self.delete_stmt, dict(path=path.replace('*', '%'))).fetchall()
                conn.commit()
            return [row.key for row in rows]
        except Exception as err:
            raise err

    def delete_many(self, paths: list) -> list:
        """ Delete many Keys """
        try:
            result = []
            with self.engine.connect() as conn:
                for path in paths:
                    rows = conn.execute(self.delete_stmt, dict(path=path.replace('*', '%'))).all()
                    result.extend([row.key for row in rows])
                conn.commit()
            return result
        except Exception as err:
            raise err

    def close(self):
        if self.engine:
            self.engine.dispose()
