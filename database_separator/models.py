from django.db import models


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
