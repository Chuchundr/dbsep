from django.db import connection
from django.core.mail import mail_admins

from email.header import Header

from project.celery import app
from project.tools.executor import Executor, connect
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


@app.task(bind=True)
def save_check_sums():
    count_general = 0
    count_db = 0
    count_tables = 0
    for db in models.DataBase.objects.all():
        connection = connect(db.name)
        database = Executor(**connection)
        tables = database.get_tables()
        for table in tables:
            count_tables += database.execute(f"select count(*) from {table}")[0][0]
        count_db += count_tables
    count_general += count_db
    models.CheckSum.objects.create(checksum=count_general)