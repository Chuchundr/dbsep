from django.db import connection
from django.core.mail import mail_admins

from project.celery import app
from database_separator import models


@app.task(bind=True)
def check_replication_slots(self):
    inactive_slots = list()
    with connection.cursor() as cursor:
        cursor.execute('select slot_name, active from pg_replication_slots')
        result = cursor.fetchall()

    for slot in result:
        if not slot[1]:
            models.ReplicationSlots.objects.get(name=slot[0]).update(active=slot[1])
            inactive_slots.append(slot[0])

    if bool(inactive_slots):
        subject = Header("{}. Notify Exception.".format(settings.APP_NAME), 'utf-8')
        slots = [', '.join(key) for key in inactive_slots] if len(inactive_slots) > 1 else inactive_slots[0]
        message = f"Обнаружены неактивные слоты: \n {slots}"
        mail_admins(subject, message)