from project.tools.executor import Executor, connect
from project.tools.patterns import Singleton
from project.tools.chain import CreateSubscription, DropSubscription


load_dotenv()


class ActionSetForRelaunch:
    """
    Сценарий
    """