import os

from dotenv import load_dotenv

from django.conf import settings
from django.core.management import call_command
from .models import SequenceRange, DataBase

from project.tools.executor import Executor, connect
from project.tools.chain import ChangeDataType, Truncate, AlterSequence, CreatePublication, CreateReplicationSlot, \
    CreateSubscription, DropTablesInPub, AddTablesToPub, DropSubscription, DropPublication


load_dotenv()


class MyIterationError(Exception):
    """
    Класс ошибки в случае, если количество итераций вышло за пределы лимита
    """
    pass


class ActionSet:
    """
    Сценарий
    """
    _sequence_range = SequenceRange.get_next_range()

    def __init__(self, app_name, db_name):
        self.db_name = db_name
        self.main_conn = connect(settings.MAIN_DB)
        self.app_conn = connect(db_name)

        self.change_data_type_app = ChangeDataType(
            type='bigint',
            fields_list=settings.DATA_TYPE_CHANGE_TABLES,
            **self.app_conn
        )
        self.add_tables_main = AddTablesToPub(
            pubname='office_main_publication',
            app_name = app_name,
            **self.main_conn
        )
        self.create_slot_main = CreateReplicationSlot(
            pubdb=settings.MAIN_DB,
            subdb=db_name,
            option='main',
            **self.main_conn
        )
        self.truncate_dct_app = Truncate(
            table='django_content_type',
            **self.app_conn
        )
        self.truncate_ap_app = Truncate(
            table='auth_permission',
            **self.app_conn
        )
        self.alter_sequnce_app = AlterSequence(
            tables=settings.SEQUENCE_CHANGE_TABLES,
            start_value=self._sequence_range.get('start'),
            max_value=self._sequence_range.get('max'),
            **self.app_conn
        )

        self.create_sub_app = CreateSubscription(
            option='main',
            subdb=db_name,
            pubdb=settings.MAIN_DB,
            pubname='office_main_publication',
            sub_connection=self.main_conn,
            **self.app_conn
        )
        self.drop_tables_main = DropTablesInPub(
            pubname='office_main_publication',
            app_name=app_name,
            **self.main_conn
        )
        
        self.create_pub_app = CreatePublication(
            tables=settings.VIEWFLOW_TABLES,
            pubdb=db_name,
            subdb=settings.MAIN_DB,
            **self.app_conn
        )
        self.create_slot_app = CreateReplicationSlot(
            pubdb=db_name,
            subdb=settings.MAIN_DB,
            **self.app_conn
        )
        self.create_sub_main = CreateSubscription(
            subdb=settings.MAIN_DB,
            pubdb=db_name,
            pubname=f'{db_name}_{settings.MAIN_DB}_sub',
            sub_connection=self.app_conn,
            **self.main_conn
        )
        self.drop_sub_main_app = DropSubscription(
            pubdb=settings.MAIN_DB,
            subdb=db_name,
            option='main',
            **self.app_conn
        )
        self.drop_sub_main = DropSubscription(
            pubdb=db_name,
            subdb=settings.MAIN_DB,
            **self.main_conn
        )

        self.create_slot_app_main = CreateReplicationSlot(
            pubdb=settings.MAIN_DB,
            subdb=db_name,
            option='main',
            **self.main_conn
        )
        self.create_slot_main_app = CreateReplicationSlot(
            pubdb=db_name,
            subdb=settings.MAIN_DB,
            **self.app_conn
        )

        self.recreate_sub_app_main = CreateSubscription(
            copy_data=False,
            option='main',
            subdb=db_name,
            pubdb=settings.MAIN_DB,
            pubname='office_main_publication',
            sub_connection=self.main_conn,
            **self.app_conn
        )

        self.recreate_sub_main_app = CreateSubscription(
            copy_data=False,
            subdb=settings.MAIN_DB,
            pubdb=db_name,
            pubname=f'{db_name}_{settings.MAIN_DB}_publication',
            sub_connection=self.app_conn,
            **self.main_conn
        )
        self.add_sequence = Executor.add_sequence_range(
            db_name=self.db_name,
            start=self._sequence_range.get('start'),
            max=self._sequence_range.get('max')
        )

    def execute_script(self):
        self.change_data_type_app\
            .set_next(self.add_tables_main)\
            .set_next(self.create_slot_main)\
            .set_next(self.truncate_dct_app)\
            .set_next(self.truncate_ap_app)\
            .set_next(self.alter_sequnce_app)\
            .set_next(self.create_sub_app)\
            .set_next(self.drop_tables_main)\
            .set_next(self.create_pub_app)\
            .set_next(self.create_slot_app)\
            .set_next(self.create_sub_main)

        self.change_data_type_app.handle()

        self.check_replication()

    def check_replication(self):
        """
        Рекурсивный метод, проверяет, скопировались ли данные из основной базы в периферийную
        """
        default_db = Executor(**self.main_conn)
        app_db = Executor(**self.app_conn)
        try:
            if default_db.check_records() == app_db.check_records():
                default_db.close()
                app_db.close()
                return self.continue_execution()
            default_db.close()
            app_db.close()
            self.check_replication()
        except Exception as e:
            raise Exception(e)

    def continue_execution(self):
        self.drop_sub_main_app\
            .set_next(self.drop_sub_main)\
            .set_next(self.create_slot_app_main)\
            .set_next(self.create_slot_main_app)\
            .set_next(self.recreate_sub_app_main)\
            .set_next(self.recreate_sub_main_app)\
            .set_next(self.add_sequence)

        self.drop_sub_main_app.handle()

        call_command('initialize')


class BackupActionSet:
    """
    Сценарий отката
    """
    def __init__(self, app_name, db_name):
        self.db_name = db_name
        self.app_conn = connect(db_name)
        self.main_conn = connect(settings.MAIN_DB)

        self.drop_tables = DropTablesInPub(
            pubname='office_main_publication',
            app_name=app_name,
            **self.main_conn
        )
        self.drop_sub_app_main = DropSubscription(
            pubdb=db_name,
            subdb=settings.MAIN_DB,
            **self.main_conn
        )
        self.drop_sub_main_app = DropSubscription(
            pubdb=settings.MAIN_DB,
            subdb=db_name,
            option='main',
            **self.app_conn
        )
        self.drop_pub_app = DropPublication(
            pubname=f'{db_name}_{settings.MAIN_DB}_publication',
            **self.app_conn
        )

    def backup(self):
        self.drop_tables\
            .set_next(self.drop_sub_app_main)\
            .set_next(self.drop_sub_main_app)\
            .set_next(self.drop_pub_app)

        self.drop_tables.handle()
        DataBase.objects.get(name=self.db_name).delete()


