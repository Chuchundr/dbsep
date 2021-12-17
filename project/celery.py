import os

from celery import Celery
from celery.schedules import crontab


os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'project.settings')


app = Celery('celery-dbsep')

app.config_from_object('django.conf:settings', namespace='CELERY')

app.autodiscover_tasks()


app.conf.beat_schedule = {
    'check replication slots': {
        'task': 'database_separator.tasks.check_replication_slots',
        'schedule': 300.0
    },
    'add_check_sums': {
        'task': 'database_separator.tasks.add_check_sums',
        'schedule': crontab(hour=3)
    }
}