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
        self.close()
        raise error

    def close(self):
        """
        метод, закрывающий соединение
        """
        self._connection.close()
        self._cursor.close()


class DatabaseSeparationClass(Executor):
    """
    Класс для настройки репликации и разделения БД
    """

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

    def _get_field_type_dict(self, fields_list: list):

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

    def truncate(self, table):

        self._cursor.execute(
            f"truncate {table} cascade"
        )
        self._connection.commit()

    def change_data_type(self, type, tables: dict):

        try:
            for field, tables in tables.items():
                for table in tables:
                    self._cursor.execute(
                        f"""alter table {table} alter column {field} type {type} USING {field}::{type}"""
                    )
                    self._connection.commit()

        except InFailedSqlTransaction as e:
            self._raise_error(e)

    def alter_sequence(self, tables: list, start_value, max_value=None):

        sequences = []  # список с сикуэнсами

        try:
            for table in tables:
                if max_value:
                    self._cursor.execute(
                        f"alter sequence {table + '_id_seq'} as bigint start "
                        f"with {self.cast(start_value, 'bigint')} "
                        f"maxvalue {self.cast(max_value, 'bigint')}"
                    )
                    sequences.append(table+'_id_seq')
                else:
                    self._cursor.execute(
                        f"alter table {table} alter column id type bigint;"
                        f"alter sequence {table + '_id_seq'} as bigint start with {start_value}"
                    )
                    sequences.append(table + '_id_seq')
                self._cursor.execute(
                    f"alter sequence {table + '_id_seq'} restart"
                )
            self._connection.commit()

        except InFailedSqlTransaction as e:
            self._raise_error(e)

    def create_publication(self, pub_name, tables: list):

        try:
            if len(tables) > 1:
                self._cursor.execute(
                    f"create publication {pub_name} for table {', '.join(tables)}"
                )
            else:
                self._cursor.execute(
                    f"create publication {pub_name} for table {tables[0]}"
                )
            self._connection.commit()

        except InFailedSqlTransaction as e:
            self._raise_error(error=e)
        except DuplicateObject:
            pass

    def create_replication_slot(self, slot_name):

        try:
            self._cursor.execute(
                f"select pg_create_logical_replication_slot('{slot_name}', 'pgoutput')"
            )

        except InFailedSqlTransaction as e:
            self._connection.rollback()
            self.raise_error(e)
        except DuplicateObject:
            pass

    def create_subscription(self, sub_name, pub_name, slot_name, copy_data=True, **connection):
        try:
            self._cursor.execute(f"""create subscription {sub_name}
                                 connection 'host={connection['host']}
                                 user={connection['user']}
                                 port={connection['port']}
                                 dbname={connection['database']}
                                 password={connection['password']}'
                                 publication {pub_name}
                                 with (create_slot=false, slot_name='{slot_name}', copy_data={copy_data})""")
            self._connection.commit()

        except InFailedSqlTransaction as e:
            self._raise_error(e)
        except DuplicateObject:
            pass

    def drop_tables_in_pub(self, pub_name, tables):
        try:
            self._cursor.execute(f"alter publication {pub_name} drop table {', '.join(tables)}")
            self._connection.commit()

        except InFailedSqlTransaction as e:
            self._raise_error(e)

    def add_tables_to_pub(self, pub_name, tables):
        try:
            self._cursor.execute(f"alter publication {pub_name} add table {', '.join(tables)}")
            self._connection.commit()

        except InFailedSqlTransaction as e:
            self._raise_error(e)
        except DuplicateObject as e:
            pass

    def drop_subscription(self, sub_name):
        try:
            self._cursor.execute(f"drop subscription {sub_name}")
            self._connection.commit()
        except InFailedSqlTransaction as e:
            self._raise_error(e)
        except ActiveSqlTransaction as e:
            self._raise_error(e)

    def refresh(self, sub_name, copy_data=True):
        try:
            if copy_data:
                self._cursor.execute(f"alter subscription {sub_name} refresh publication")
                self._connection.commit()
            else:
                self._cursor.execute(f"alter subscription {sub_name} refresh publication with (copy_data={copy_data})")
                self._connection.commit()
        except InFailedSqlTransaction as e:
            self._raise_error(e)

    def get_app_tables(self, detachable_app):
        self._cursor.execute(f"select table_name from information_schema.tables "
                            f"where table_name like '{detachable_app}%'")
        return [table[0] for table in self._cursor.fetchall()]

    def cast(self, value, type):
        self._cursor.execute(
            f"select cast({value} as {type})"
        )
        return self._cursor.fetchall()[0][0]