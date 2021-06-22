from django.conf import settings
from .executor import DatabaseSeparationClass
from .models import SequenceRange


SEQUENCE_CHANGE_TABLES = [
    'auth_permission',
    'django_content_type',
    'viewflow_process',
    'viewflow_task_previous',
    'viewflow_task'
]


class ActionSet:

    def __init__(self, detachable_app):
        self.office_db = DatabaseSeparationClass(**self.connection_options('office'))
        self.vehicles_db = DatabaseSeparationClass(**self.connection_options('vehicles'))
        self.detachable_db = DatabaseSeparationClass(**self.connection_options(detachable_app))
        self.detachable_app = detachable_app

    def connection_options(self, db):
        connection = settings.DATABASE_CONNECTION
        connection['database'] = db
        return connection

    def do(self):

        app_tables = self.office_db.get_app_tables(self.detachable_app)

        sequence_range = SequenceRange.get_next_range()

        self.detachable_db.change_data_type(
            type='bigint',
            tables=self.detachable_db._get_field_type_dict(
                fields_list=[
                    'from_task_id',
                    'to_task_id',
                    'process_id',
                    'process_ptr_id',
                    'task_id'
                ]
            )
        )

        self.detachable_db.change_data_type(
            type='bigint',
            tables=self.detachable_db._get_field_type_dict(['task_id', 'object_id', 'content_type_id']))

        self.detachable_db.change_data_type(
            type='bigint',
            tables={
                'id': ['viewflow_process', 'viewflow_task', 'viewflow_task_previous']
            }
        )

        self.office_db.add_tables_to_pub('office_main_publication', app_tables + ['viewflow_task',
                                                                           'viewflow_process',
                                                                           'viewflow_task_previous'])

        # очистка таблиц django_content_type, auth_permission
        list(map(self.detachable_db.truncate, ['django_content_type', 'auth_permission']))

        # изменение сиквенсов
        self.detachable_db.alter_sequence(SEQUENCE_CHANGE_TABLES, start_value=sequence_range['start'],
                                  max_value=sequence_range['max'])

        # создание публикации в дефолт
        self.office_db.create_replication_slot(f'office_{self.detachable_app}_main_slot')

        # создание подписки
        self.detachable_db.create_subscription(sub_name=f'{self.detachable_app}_office_main_sub',
                                       pub_name='office_main_publication',
                                       slot_name=f'office_{self.detachable_app}_main_slot',
                                       **self.connection_options('office'))

        # изменение сиквенсов в таблицах, имеющих записи
        for table, id_value in office_db._get_next_id_value(self.detachable_app).items():
            self.detachable_db.alter_sequence([table, ], start_value=id_value + 1)

        # удаление таблиц из публикации main

        tables = [
            'viewflow_process',
            'viewflow_task',
            'viewflow_task_previous'
        ]

        # создание подписки для репликации городов
        self.vehicles_db.create_replication_slot(f'vehicles_{self.detachable_app}_cities_slot')

        self.detachable_db.create_subscription(sub_name=f'{self.detachable_app}_vehicles_cities_sub',
                                       pub_name='vehicles_cities_publication',
                                       slot_name=f'vehicles_{self.detachable_app}_cities_slot',
                                       **self.connection_options('vehicles'))

        # создание публикации в provgov
        self.detachable_db.create_publication(pub_name=f'{self.detachable_app}_office_publication',
                                      tables=VIEWFLOW_PERIPHERAL)

        self.detachable_db.create_replication_slot(f'{self.detachable_app}_office_slot')

        # создание подписки в default
        self.office_db.create_subscription(sub_name=f'office_{self.detachable_app}_sub',
                                       pub_name=f'{self.detachable_app}_office_publication',
                                       slot_name=f'{self.detachable_app}_office_slot',
                                       copy_data=False,
                                       **self.connection_options(f'{self.detachable_app}'))

        self.office_db.refresh(sub_name='office_provgov_sub', copy_data=False)
        self.detachable_db.refresh(sub_name=f'{self.detachable_app}_vehicles_cities_sub', copy_data=False)

        self.office_db.drop_tables_in_pub('office_main_publication', app_tables + ['viewflow_process',
                                                                            'viewflow_task',
                                                                            'viewflow_task_previous'])

        self.office_db.close()
        self.vehicles_db.close()
        self.detachable_db.close()