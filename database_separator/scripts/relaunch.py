from project.tools.executor import Executor, connect
from project.tools.patterns import Singleton
from project.tools.chain import CreateSubscription, DropSubscription
from database_separator.models import DataBase, Subscription, ReplicationSlot


load_dotenv()


class ActionSetForRelaunch:
    """
    Сценарий
    """
    def relaunch(self):
        for db in DataBase.objects.all():
            action = DropSubscription(
                
            )