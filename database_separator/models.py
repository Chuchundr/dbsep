from django.db import models
from django.db.utils import ProgrammingError


class DataBase(models.Model):
    name = models.CharField('Название БД', max_length=250,)

    def __str__(self):
        return self.name

    class Meta:
        verbose_name = 'База данных'
        verbose_name_plural = 'Базы данных'


class Publication(models.Model):
    database = models.ForeignKey(DataBase, on_delete=models.CASCADE, verbose_name='БД', related_name='publications')
    name = models.CharField('Название публикации', max_length=250)

    def __str__(self):
        return self.name

    class Meta:
        verbose_name = 'Публикация'
        verbose_name_plural = 'Публикации'


class PubTables(models.Model):
    pub = models.ForeignKey(Publication, on_delete=models.CASCADE, verbose_name='Публикация', related_name='tables')
    name = models.CharField('Таблица в публикации', max_length=250, blank=True)

    def __str__(self):
        return self.name


class Subscription(models.Model):
    database = models.ForeignKey(DataBase, on_delete=models.CASCADE, verbose_name='БД', related_name='subscriptions')
    name = models.CharField('Название подписки', max_length=250)
    to_database = models.ForeignKey(DataBase, on_delete=models.CASCADE, verbose_name='к БД', related_name='sub_to_me',
                                    null=True)

    def __str__(self):
        return self.name

    class Meta:
        verbose_name = 'Подписка'
        verbose_name_plural = 'Подписки'


class ReplicationSlot(models.Model):
    database = models.ForeignKey(DataBase, on_delete=models.CASCADE, verbose_name='БД',
                                 related_name='replication_slots')
    name = models.CharField('Название слота', max_length=250)
    active = models.BooleanField('Активен', null=True)

    def __str__(self):
        return self.name

    class Meta:
        verbose_name = 'Слот репликации'
        verbose_name_plural = 'Слоты репликации'


class SequenceRange(models.Model):
    start = models.IntegerField('Начальное значение')
    max = models.IntegerField('Конечное значение')
    database = models.CharField('БД', max_length=250, null=True, blank=True)
    number = models.PositiveIntegerField('Порядковый номер', blank=True, null=True)

    def __str__(self):
        return f'Диапазон для {self.database}'

    @classmethod
    def get_next_range(cls):
        try:
            last_range = cls.objects.order_by('number').last()
            start = last_range.start/10**16
            max = last_range.max/10**16
            return {'start': (start + 2)*10**16, 'max': (max + 2)*10**16 - 1}
        except:
            return {'start': '', 'max': ''}

    class Meta:
        verbose_name = 'Диапазон для БД'
        verbose_name_plural = 'Диапазоны для БД'


class CheckSum(models.Model):
    checksum = models.IntegerField('Контрольная сумма')
    date = models.DateTimeField('Время добавления', auto_now_add=True)

    class Meta:
        verbose_name = 'Контрольная сумма'
        verbose_name_plural = 'Контрольные суммы'