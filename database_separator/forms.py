from psycopg2 import connect as pgconnect

from django import forms

from .operations import connect
from .models import DataBase
from .operations import ActionSet


class SeparationForm(forms.Form):

    app_name = forms.CharField(label='Название приложения', required=True)
    db_name = forms.CharField(label='Имя БД', required=True)

    def clean(self):
        cleaned_data = super().clean()
        try:
            options = connect(cleaned_data.get('db_name'))
            pgconnect(**options)
        except Exception:
            raise forms.ValidationError({'db_name': 'Приложения или БД с таким именем не существует'})

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['app_name'].widget.attrs.update({'class': 'form-control'})
        self.fields['db_name'].widget.attrs.update({'class': 'form-control'})
