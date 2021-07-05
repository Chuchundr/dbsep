from psycopg2 import connect as pgconnect

from django import forms

from .tools import connect
from .models import DataBase


class SeparationForm(forms.Form):
    """
    Форма для раздления
    Поля:
        app_name - название приложения
        db_name - название БД
    """

    app_name = forms.CharField(label='Название приложения', required=True)
    db_name = forms.CharField(label='Имя БД', required=True)

    def clean(self):
        """
        Проверка на валидацию
        :return:
        """
        cleaned_data = super().clean()
        if DataBase.objects.filter(name=cleaned_data.get('db_name')).exists():
            raise forms.ValidationError({'db_name': 'Для указанной базы уже проведены настройки репликации'})
        try:
            options = connect(cleaned_data.get('db_name'))
            pgconnect(**options)
        except Exception:
            raise forms.ValidationError({'db_name': 'Приложения или БД с таким именем не существует'})

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['app_name'].widget.attrs.update({'class': 'form-control'})
        self.fields['db_name'].widget.attrs.update({'class': 'form-control'})
