from .executor import Executor


class BaseHandler(Executor):
    _next_handler = None

    def set_next(self, handler):
        self._next_handler = handler
        return handler

    def handle(self, *args, **kwargs):
        if self._next_handler:
            return self._next_handler.handle(*args, **kwargs)
        return None


class ChangeDataType(BaseHandler, Executor):

    def __init__(self, type:str, fields_list: list, **connection):
        super().__init__(**connection)
        self.tables = self._get_field_type_dict(fields_list)
        self.type = type

    def handle(self):
        try:
            for field, tables in self.tables.items():
                for table in tables:
                    self._cursor.execute(
                        f"""alter table {table} alter column {field} type {self.type} USING {field}::{self.type}"""
                    )
            self._connection.commit()
        except Exception as e:
            self._connection.rollback()
            self.close()
            self.raise_error(e)
        finally:
            self.close()
        return super().handle()


class Truncate(BaseHandler, Executor):

    def __init__(self, table:str, **connection):
        super().__init__(**connection)
        self.table = table

    def handle(self):
        try:
            self._cursor.execute(
                f"truncate {self.table} cascade"
            )
            self._connection.commit()
        except Exception as e:
            self._connection.rollback()
            self.close()
            self.raise_error(e)
        finally:
            self.close()
        return super().handle()


class AlterSequence(BaseHandler, Executor):

    def __init__(self, tables: list, start_value, max_value=None, **connection):
        super().__init__(**connection)
        self.tables = tables
        self.start_value = start_value
        self.max_value = max_value

    def handle(self):
        try:
            for table in self.tables:
                if self.max_value:
                    self._cursor.execute(
                        f"alter sequence {table + '_id_seq'} as bigint start "
                        f"with {self.cast(self.start_value, 'bigint')} "
                        f"maxvalue {self.cast(self.max_value, 'bigint')}"
                    )
                else:
                    self._cursor.execute(
                        f"alter table {table} alter column id type bigint;"
                        f"alter sequence {table + '_id_seq'} as bigint start with {self.start_value}"
                    )
                self._cursor.execute(
                    f"alter sequence {table + '_id_seq'} restart"
                )
            self._connection.commit()

        except Exception as e:
            self._connection.rollback()
            self.close()
            self.raise_error(e)
        finally:
            self.close()
        return super().handle()


class CreatePublication(BaseHandler, Executor):
    def __init__(self, tables: list, pubdb: str, subdb: str = None, **connection):
        super().__init__(**connection)
        self.tables = tables
        self.pubname = f'{pubdb}_{subdb}_publication'

    def handle(self):
        try:
            if len(self.tables) > 1:
                self._cursor.execute(
                    f"create publication {self.pubname} for table {', '.join(self.tables)}"
                )
            else:
                self._cursor.execute(
                    f"create publication {self.pubname} for table {self.tables[0]}"
                )
            self._connection.commit()
        except Exception as e:
            self._connection.rollback()
            self.close()
            self.raise_error(e)
        finally:
            self.close()
        return super().handle()


class CreateReplicationSlot(BaseHandler, Executor):
    def __init__(self, pubdb, subdb, option=None, **connection):
        super().__init__(**connection)
        if option:
            self.slot_name = f'{pubdb}_{subdb}_{option}_slot'
        else:
            self.slot_name = f'{pubdb}_{subdb}_slot'

    def handle(self):
        try:
            self._cursor.execute(
                f"select pg_create_logical_replication_slot('{self.slot_name}', 'pgoutput')"
            )
        except Exception as e:
            self._connection.rollback()
            self.close()
            self.raise_error(e)
        finally:
            self.close()
        return super().handle()


class CreateSubscription(BaseHandler, Executor):

    def __init__(self, subdb, pubdb, pubname, sub_connection: dict, option=None, copy_data=True, **connection):
        super().__init__(**connection)
        if option:
            self.subname = f'{subdb}_{pubdb}_{option}_sub'
            self.slot_name = f'{pubdb}_{subdb}_{option}_slot'
        else:
            self.subname = f'{subdb}_{pubdb}_sub'
            self.slot_name = f'{pubdb}_{subdb}_slot'
        self.copy_data = copy_data
        self.sub_connection = sub_connection
        self.pubname = pubname

    def handle(self):
        try:
            self._cursor.execute(f"""create subscription {self.subname}
                                 connection 'host={self.sub_connection['host']}
                                 user={self.sub_connection['user']}
                                 port={self.sub_connection['port']}
                                 dbname={self.sub_connection['database']}
                                 password={self.sub_connection['password']}'
                                 publication {self.pubname}
                                 with (create_slot=false, slot_name='{self.slot_name}', copy_data={self.copy_data})""")
            self._connection.commit()
        except Exception as e:
            self._connection.rollback()
            self.close()
            self.raise_error(e)
        finally:
            self.close()
        return super().handle()


class DropTablesInPub(BaseHandler, Executor):
    def __init__(self, pubname, app_name, **connection):
        super().__init__(**connection)
        self.pubname = pubname
        self.tables = self._get_app_tables(app_name)

    def handle(self):
        try:
            self._cursor.execute(f"alter publication {self.pubname} drop table {', '.join(self.tables)}")
            self._connection.commit()
        except Exception as e:
            self._connection.rollback()
            self.close()
            self.raise_error(e)
        finally:
            self.close()
        return super().handle()


class AddTablesToPub(BaseHandler, Executor):
    def __init__(self, pubname, app_name, **connection):
        super().__init__(**connection)
        self.pubname = pubname
        self.tables = self._get_app_tables(app_name)

    def handle(self):
        try:
            self._cursor.execute(f"alter publication {self.pubname} add table {', '.join(self.tables)}")
            self._connection.commit()
        except Exception as e:
            self._connection.rollback()
            self.close()
            self.raise_error(e)
        finally:
            self.close()
        return super().handle()


class DropSubscription(BaseHandler, Executor):
    def __init__(self, subname, **connection):
        super().__init__(**connection)
        self.subname = subname

    def handle(self):
        try:
            self._cursor.execute(f"drop subscription {self.subname}")
            self._connection.commit()
        except Exception as e:
            self._connection.rollback()
            self.close()
            self.raise_error(e)
        finally:
            self.close()
        return super().handle()