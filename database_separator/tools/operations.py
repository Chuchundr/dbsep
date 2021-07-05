import os
import time
from dotenv import load_dotenv

from django.conf import settings
from django.core.management import call_command
from ..models import SequenceRange, DataBase

from .executor import Executor
from .chain import ChangeDataType, Truncate, AlterSequence, CreatePublication, CreateReplicationSlot, \
    CreateSubscription, DropTablesInPub, AddTablesToPub, DropSubscription


load_dotenv()

DATA_TYPE_CHANGE_TABLES = [
    'from_task_id',
    'to_task_id',
    'process_id',
    'process_ptr_id',
    'task_id',
    'id',
    'object_id',
    'content_type_id'
]

SEQUENCE_CHANGE_TABLES = [
    'auth_permission',
    'django_content_type',
    'viewflow_process',
    'viewflow_task_previous',
    'viewflow_task'
]

VIEWFLOW_TABLES = [
    'viewflow_process',
    'viewflow_task'
]


def connect(db_name: str) -> dict:
    options = {
            'user': os.getenv('DATABASE_USERNAME'),
            'port': os.getenv('DATABASE_PORT'),
            'host': os.getenv('DATABASE_HOST'),
            'database': db_name,
            'password': os.getenv('DATABASE_PASSWORD')
        }
    return options


class ActionSet:

    _sequence_range = SequenceRange.get_next_range()

    def __init__(self, app_name, db_name):
        self.db_name = db_name
        self.vehicles_conn = connect(settings.VEHICLES_DB)
        self.main_conn = connect(settings.MAIN_DB)
        self.app_conn = connect(db_name)

        self.change_data_type_app = ChangeDataType(
            type='bigint',
            fields_list=DATA_TYPE_CHANGE_TABLES,
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
            tables=SEQUENCE_CHANGE_TABLES,
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
        self.create_slot_vehicles = CreateReplicationSlot(
            pubdb=settings.VEHICLES_DB,
            subdb=db_name,
            option='cities',
            **self.vehicles_conn
        )
        self.create_cities_sub_app = CreateSubscription(
            subdb=db_name,
            pubdb=settings.VEHICLES_DB,
            pubname='vehicles_cities_publication',
            option='cities',
            sub_connection=self.vehicles_conn,
            **self.app_conn
        )
        self.create_pub_app = CreatePublication(
            tables=VIEWFLOW_TABLES,
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
            pubname=f'{app_name}_{settings.MAIN_DB}_sub',
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
        self.drop_sub_cities_vehicles = DropSubscription(
            pubdb=settings.VEHICLES_DB,
            subdb=db_name,
            option='cities',
            **self.app_conn
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
        self.create_slot_vehicles_app = CreateReplicationSlot(
            pubdb=settings.VEHICLES_DB,
            subdb=db_name,
            option='cities',
            **self.vehicles_conn
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
        self.recreate_cities_sub_app = CreateSubscription(
            copy_data=False,
            subdb=db_name,
            pubdb=settings.VEHICLES_DB,
            pubname='vehicles_cities_publication',
            option='cities',
            sub_connection=self.vehicles_conn,
            **self.app_conn
        )
        self.recreate_sub_main_app = CreateSubscription(
            copy_data=False,
            subdb=settings.MAIN_DB,
            pubdb=db_name,
            pubname=f'{app_name}_{settings.MAIN_DB}_publication',
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
            .set_next(self.create_slot_vehicles)\
            .set_next(self.create_cities_sub_app)\
            .set_next(self.create_pub_app)\
            .set_next(self.create_slot_app)\
            .set_next(self.create_sub_main)\
            .set_next(self.drop_sub_main_app)\
            .set_next(self.drop_sub_main)\
            .set_next(self.drop_sub_cities_vehicles)\
            .set_next(self.create_slot_app_main)\
            .set_next(self.create_slot_main_app)\
            .set_next(self.create_slot_vehicles_app)\
            .set_next(self.recreate_sub_app_main)\
            .set_next(self.recreate_cities_sub_app)\
            .set_next(self.recreate_sub_main_app)

        self.change_data_type_app.handle()

        time.sleep(3)

        call_command('initialize')


class BackupActionSet:
    def __init__(self, app_name, db_name):
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
        self.drop_sub_app_vehicles = DropSubscription(
            pubdb=settings.VEHICLES_DB,
            subdb=db_name,
            option='cities',
            **self.app_conn
        )
        self.drop_sub_main_app = DropSubscription(
            pubdb=settings.MAIN_DB,
            subdb=db_name,
            option='main',
            **self.app_conn
        )

    def backup(self):
        self.drop_tables\
            .set_next(self.drop_sub_app_main)\
            .set_next(self.drop_sub_main_app)\
            .set_next(self.drop_sub_app_vehicles)

        self.drop_tables.handle()


