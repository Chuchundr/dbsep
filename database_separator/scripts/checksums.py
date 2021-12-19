from database_separator import models
from project.tools.executor import Executor, connect


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