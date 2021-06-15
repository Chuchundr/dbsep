# Generated by Django 3.0.5 on 2021-06-15 07:16

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('database_separator', '0001_initial'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='publication',
            options={'verbose_name': 'Публикация', 'verbose_name_plural': 'Публикации'},
        ),
        migrations.AlterModelOptions(
            name='replicationslot',
            options={'verbose_name': 'Слот репликации', 'verbose_name_plural': 'Слоты репликации'},
        ),
        migrations.AlterModelOptions(
            name='subscription',
            options={'verbose_name': 'Подписка', 'verbose_name_plural': 'Подписки'},
        ),
        migrations.AddField(
            model_name='replicationslot',
            name='active',
            field=models.BooleanField(null=True, verbose_name='Активен'),
        ),
    ]
