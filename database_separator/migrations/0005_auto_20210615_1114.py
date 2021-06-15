# Generated by Django 3.0.5 on 2021-06-15 11:14

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('database_separator', '0004_remove_publication_tables'),
    ]

    operations = [
        migrations.AlterField(
            model_name='pubtables',
            name='pub',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='tables', to='database_separator.Publication', verbose_name='Публикация'),
        ),
    ]
