from dotenv import load_dotenv
from project.tools.executor import Executor, connect
from project.tools.patterns import Singleton
from project.tools.chain import CreateSubscription, DropSubscription, CreateReplicationSlot
from database_separator.models import DataBase, Subscription, ReplicationSlot


load_dotenv()


class ActionSetForRelaunch:
    """
    Сценарий
    """
    def execute_script(self):
        for db in DataBase.objects.all():
            subdb = db.name
            for d in db.subscriptions.all():
                pubname = 'office_main_publication' if subdb != 'office' \
                    else f'{d.to_database.name}_{subdb}_publication'
                drop_sub = DropSubscription(
                    subdb=subdb,
                    pubdb=d.to_database.name,
                    option='main' if subdb != 'office' else None,
                    **connect(subdb)
                )
                create_sub = CreateSubscription(
                    subdb=subdb,
                    pubdb=d.to_database.name,
                    pubname=pubname,
                    sub_connection=connect(d.to_database.name),
                    option='main' if subdb != 'office' else None,
                    copy_data=False,
                    **connect(subdb)
                )
                create_slots = CreateReplicationSlot(
                    subdb=subdb,
                    pubdb=d.to_database.name,
                    option='main' if subdb != 'office' else None,
                    **connect(d.to_database.name)
                )

                drop_sub.set_next(create_sub).set_next(create_slots)

                drop_sub.handle()