import psycopg2
import logging


class Executor:

    def __init__(self, **connection):
        """
        задаём атрибуты connection, для соединения,
        и cursor, для запуска команд
        """
        self.connection = psycopg2.connect(**connection)
        self.cursor = self.connection.cursor()

    def raise_error(self, error):
        """
        метод для обработки ошибок,
        откатывает транзакцию и закрывает соединение
        """
        self.connection.rollback()
        self.close()
        raise error

    def close(self):
        """
        метод, закрывающий соединение
        """
        self.connection.commit()
        self.connection.close()
        self.cursor.close()