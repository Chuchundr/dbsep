from django.core.management.base import BaseCommand
from django.db import connection

from database_separator.models import SequenceRange, DataBase


class Command(BaseCommand):

    def handle(self, **options):
        with connection.cursor() as cursor:
            cursor.execute(
                "alter table database_separator_sequencerange alter column start type bigint using start::bigint;"
                "alter table database_separator_sequencerange alter column max type bigint using max::bigint;"
            )
        for db in DataBase.objects.all():
            if db.name == 'office':
                SequenceRange.objects.update_or_create(start=0, max=(1*10**16)-1, database=db, number=1)
            if db.name == 'pass':
                SequenceRange.objects.update_or_create(start=1*10**16, max=(3*10**16)-1, database=db, number=2)
            if db.name == 'vehicles':
                SequenceRange.objects.update_or_create(start=3*10**16, max=(5*10**16)-1, database=db, number=3)
            if db.name == 'cellular':
                SequenceRange.objects.update_or_create(start=6*10**16, max=(7*10**16)-1, database=db, number=4)
            if db.name == 'documentflow':
                SequenceRange.objects.update_or_create(start=8*10**16, max=(9*10**16)-1, database=db, number=5)
            if db.name == 'provgov':
                SequenceRange.objects.update_or_create(start=10*10**16, max=(11*10**16)-1, database=db, number=6)
