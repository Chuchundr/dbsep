import psycopg2
import logging
from psycopg2.errors import InFailedSqlTransaction, \
    DuplicateObject, UndefinedTable, InvalidTextRepresentation, ActiveSqlTransaction


class Executor:

    def __init__(self, **connection):
        """
        задаём атрибуты connection, для соединения,
        и cursor, для запуска команд
        """
        self._connection = psycopg2.connect(**connection)
        self._cursor = self._connection.cursor()

    def raise_error(self, error):
        """
        метод для обработки ошибок,
        откатывает транзакцию и закрывает соединение
        """
        raise error

    def close(self):
        """
        метод, закрывающий соединение
        """
        self._connection.close()
        self._cursor.close()

    def _get_app_tables(self, appname):
        self._cursor.execute(f"select table_name from information_schema.tables "
                             f"where table_name like '{appname}%'")
        return [table[0] for table in self._cursor.fetchall()]

    def _get_field_type_dict(self, fields_list):
        self._cursor.execute(
            f"""
            select column_name, table_name from information_schema.columns
            where column_name in ('{"', '".join(fields_list)}')
            """
        )
        types_dict = {}
        for rec in self._cursor.fetchall():
            if rec[0] in types_dict:
                types_dict[rec[0]].append(rec[1])
            else:
                types_dict[rec[0]] = [rec[1], ]
        return types_dict

    def _get_next_id_value(self, app):
        self._cursor.execute(
            f"""
            select table_name from information_schema.tables
            where table_name like '{app}%'
            """
        )
        tables = self._cursor.fetchall()

        sequences = {}

        for table in tables:
            try:
                self._cursor.execute(
                    f"""
                    select last_value from {table[0]}_id_seq
                    """
                )
                sequences[table[0]] = self._cursor.fetchone()[0]
            except UndefinedTable:
                self._connection.rollback()
                pass
        return sequences

    def cast(self, value, type):
        self._cursor.execute(
            f"select cast({value} as {type})"
        )
        return self._cursor.fetchone()[0]