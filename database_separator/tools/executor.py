import psycopg2
import logging
from psycopg2 import extensions
from psycopg2.errors import InFailedSqlTransaction, \
    DuplicateObject, UndefinedTable, InvalidTextRepresentation, ActiveSqlTransaction, UndefinedObject

from ..models import SequenceRange, DataBase


class Executor:

    def __init__(self, **connection):
        """
        задаём атрибуты connection, для соединения,
        и cursor, для запуска команд
        """
        self._connection = psycopg2.connect(**connection)
        self._connection.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        self._cursor = self._connection.cursor()

    def execute(self, query: str):
        """
        функция для выполнения запросов
        :param query: запрос
        """
        try:
            self._cursor.execute(query)
        except UndefinedObject:
            pass
        except DuplicateObject:
            pass
        except Exception as e:
            self._connection.rollback()
            self.raise_error(e)
            self.close()
        else:
            self._connection.commit()

    def raise_error(self, error: Exception):
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

    def _get_app_tables(self, appname: str) -> list:
        """
        Возвращает список таблиц для заданного приложения
        :param appname: название приложения
        :return: list()
        """
        self._cursor.execute(f"select table_name from information_schema.tables "
                             f"where table_name like '{appname}%'")
        return [table[0] for table in self._cursor.fetchall()]

    def _get_field_type_dict(self, fields_list: list) -> dict:
        """
        возвращает словарь вида {'название поля': [список таблиц, где есть данное поле]}
        :param fields_list: список с названиями полей
        :return: dict
        """
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

    def _get_next_id_value(self, app: str) -> dict:
        """
        возвращает следующее значение id для таблиц заданного приложения вида {'название таблицы': число}
        :param app: название приложения
        :return: dict
        """
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

    def cast(self, value: str, type: str) -> int:
        """
        преобразование числа с экспоненциального вида в обычный
        :param value: значение числа
        :param type: тип
        :return: число типа type
        """
        self._cursor.execute(
            f"select cast({value} as {type})"
        )
        return self._cursor.fetchone()[0]

    @staticmethod
    def add_sequence_range(db_name: str, start: int, max: int):
        DataBase.objects.get_or_create(name=db_name)
        number = SequenceRange.objects.order_by('number').last().number + 1
        SequenceRange.objects.create(database=db_name, start=start, max=max, number=number)