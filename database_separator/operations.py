from django.conf import settings
from .models import SequenceRange
from .chain import ChangeDataType, Truncate, AlterSequence, CreatePublication, CreateReplicationSlot, \
    CreateSubscription, DropTablesInPub, AddTablesToPub, DropSubscription


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


def connect(db_name):
    options = settings.DATABASE_CONNECTION
    options['database'] = db_name
    return options


class ActionSet:

    _sequence_range = SequenceRange.get_next_range()

    def __init__(self, app_name, db_name):
        self.change_data_type_app = ChangeDataType(
            type='bigint',
            fields_list= DATA_TYPE_CHANGE_TABLES,
            **connect(db_name)
        )
        self.add_tables_main = AddTablesToPub(
            pubname='office_main_publication',
            app_name = app_name,
            **connect(settings.MAIN_DB)
        )
        self.create_slot_main = CreateReplicationSlot(
            pubdb=settings.MAIN_DB,
            subdb=db_name,
            option='main',
            **connect(settings.MAIN_DB)
        )
        self.truncate_dct_app = Truncate(
            table='django_content_type',
            **connect(db_name)
        )
        self.truncate_ap_app = Truncate(
            table='auth_permission',
            **connect(db_name)
        )
        self.alter_sequnce_app = AlterSequence(
            tables = SEQUENCE_CHANGE_TABLES,
            start_value=self._sequence_range.get('start'),
            max_value=self._sequence_range.get('max'),
            **connect(db_name)
        )
        self.create_sub_app = CreateSubscription(
            option='main',
            subdb=db_name,
            pubdb=settings.MAIN_DB,
            pubname='office_main_publication',
            sub_connection = connect(settings.MAIN_DB),
            **connect(db_name)
        )
        self.drop_tables_main = DropTablesInPub(
            pubname='office_main_publication',
            app_name=app_name,
            **connect(settings.MAIN_DB)
        )
        self.create_slot_vehicles = CreateReplicationSlot(
            pubdb=settings.VEHICLES_DB,
            subdb=db_name,
            option='cities',
            **connect(settings.VEHICLES_DB)
        )
        self.create_cities_sub_app = CreateSubscription(
            subdb=db_name,
            pubdb=settings.VEHICLES_DB,
            pubname='vehicles_cities_publication',
            option='cities',
            sub_connection=connect(settings.VEHICLES_DB),
            **connect(db_name)
        )
        self.create_pub_app = CreatePublication(
            tables=VIEWFLOW_TABLES,
            pubdb=db_name,
            subdb=settings.MAIN_DB,
            **connect(db_name)
        )
        self.create_slot_app = CreateReplicationSlot(
            pubdb=db_name,
            subdb=settings.MAIN_DB,
            **connect(db_name)
        )
        self.create_sub_main = CreateSubscription(
            subdb=settings.MAIN_DB,
            pubdb=db_name,
            pubname=f'{app_name}_{settings.MAIN_DB}_sub',
            sub_connection=connect(db_name),
            **connect(settings.MAIN_DB)
        )

    def start(self):
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
            .set_next(self.create_sub_main)

        self.change_data_type_app.handle()

# 1. изменение типа данных полей на bigint (app)
# 2. Добавляем таблицы в публикацию офис  (office)
# 3. очистка таблиц django_content_type, auth_permission (app)
# 4. изменение сиквенсов  (app)
# 5. создание подписки (app)
# 6. изменение сиквенсов в таблицах, имеющих записи (app)
# 7. удаление таблиц из публикации main (office)
# 8. создание слота для репликации городов (vehicles)
# 9. создание публикации в app (app)
# 10. создание слота в для публикации (app)
# 11. создание подписки в default (office)
# 12. Удалить и пересоздать подписки (app, vehicles, office)


