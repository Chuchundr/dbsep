from django.db import connection
from django.core.mail import mail_admins

from email.header import Header

from project.celery import app
from database_separator import models


@app.task(bind=True)
def check_replication_slots(self):
    inactive_slots = list()
    with connection.cursor() as cursor:
        cursor.execute('select slot_name, active from pg_replication_slots')
        result = cursor.fetchall()

    for slot in result:
        repl_slot = models.ReplicationSlot.objects.get(name=slot[0])
        repl_slot.active = slot[1]
        repl_slot.save()
        if not slot[1]:
            inactive_slots.append(slot[0])
    if bool(inactive_slots):
        subject = Header("Replication. Notify Exception." 'utf-8')
        slots = [', '.join(key) for key in inactive_slots] if len(inactive_slots) > 1 else inactive_slots[0]
        message = f"Обнаружены неактивные слоты: \n {slots}"
        mail_admins(subject, message)