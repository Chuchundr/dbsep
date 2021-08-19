import os
import re
import psycopg2
from django.core.management.base import BaseCommand
from django.conf import settings
from django.db import connection
from django.db.utils import DatabaseError
from django.core.exceptions import ObjectDoesNotExist

from project.tools import Executor
from database_separator import models


class Command(BaseCommand):
    PORT = connection.cursor().db.settings_dict['PORT']

    def execute_query(self, query):
        """
        Выполняет переданный в аргументы запрос
        :param query: запрос
        :return: вывод
        """
        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
        except DatabaseError:
            return []
        return result

    def get_databases(self):
        """
        Возвращает список баз данных в кластере
        :return:
        """
        databases = self.execute_query(
            "select datname from pg_database"
        )
        return [db[0] for db in databases]

    def get_publications(self):
        """
        Возвращает словарь, где ключ - база данных, а значение - список публикаций в данной базе
        :return: dict
        """
        pubs_dict = dict()
        dbs = self.get_databases()
        for db in dbs:
            query = f"select pubname from dblink('dbname={db} port={self.PORT}', 'select pubname from pg_publication') \
                    as pg_publication(pubname varchar)"
            pubs = [pub[0] for pub in self.execute_query(query)]
            pubs_dict[db] = pubs
        return pubs_dict

    def get_publication_tables(self):
        """
        Возвращает словарь, где ключ - публикация, значение - список таблиц в публикации
        :return: dict
        """
        pubs_tables = dict()
        dbs = self.get_databases()
        for db in dbs:
            query = f"select * " \
                    f"from dblink('dbname={db} port={self.PORT}', 'select pubname, tablename from pg_publication_tables')" \
                    f"as pg_publication_tables(pubname varchar, tablename varchar)"
            result = self.execute_query(query)
            for rec in result:
                if not pubs_tables.get(rec[0]):
                    pubs_tables[rec[0]] = [i[1] for i in result if i[0] == rec[0]]  # {'pubname': [tables, ]}
                else:
                    pass
        return pubs_tables

    def get_subscriptions(self):
        subs_dict = dict()
        dbs = self.get_databases()
        query = "select subname from pg_subscription"
        subs = [slot[0] for slot in self.execute_query(query)]
        for db in dbs:
            subs_dict[db] = [sub for sub in subs if re.search(f'^{db}', sub)]  # если слот начинается с имени базы
        return subs_dict

    def get_replication_slots(self):
        slots_dict = dict()
        dbs = self.get_databases()
        query = "select slot_name, active from pg_replication_slots"
        slots = [slot for slot in self.execute_query(query)]
        for db in dbs:
            slots_dict[db] = [slot for slot in slots if re.search(f'^{db}', slot[0])]
        return slots_dict

    def delete_unnecessary_dbs(self):
        for db in models.DataBase.objects.all():
            if not db.publications.exists():
                db.delete()

    def handle(self, *args, **options):
        databases = self.get_databases()
        pubs = self.get_publications()
        subs = self.get_subscriptions()
        slots = self.get_replication_slots()
        pub_tables = self.get_publication_tables()

        for db in databases:
            models.DataBase.objects.update_or_create(name=db)

        for db, subs in subs.items():
            for sub in subs:
                try:
                    models.Subscription.objects.update_or_create(
                        name=sub,
                        database=models.DataBase.objects.get(name=db)
                    )
                except ObjectDoesNotExist:
                    pass

        for db, pubs in pubs.items():
            for pub in pubs:
                models.Publication.objects.update_or_create(
                    name=pub,
                    database=models.DataBase.objects.get(name=db)
                )

        for db, slots in slots.items():
            for slot in slots:
                try:
                    my_slot = models.ReplicationSlot.objects.update_or_create(
                        name=slot[0],
                        database=models.DataBase.objects.get(name=db)
                    )[0]
                    my_slot.active = slot[1]
                    my_slot.save()
                except ObjectDoesNotExist:
                    pass

        for pub, tables in pub_tables.items():
            for tab in tables:
                publication = models.Publication.objects.get(name=pub)
                table = models.PubTables.objects.update_or_create(pub=publication, name=tab)
                if not publication.tables.get(name=table[0].name):
                    pub.tables.create(pub=pub, name=table)
        self.delete_unnecessary_dbs()
