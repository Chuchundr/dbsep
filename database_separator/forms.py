from django import forms

from .models import DataBase
from .operations import ActionSet


class SeparationForm(forms.Form):

    app_name = forms.CharField(label='Название приложения', required=True)

    def clean(self):
        cleaned_data = super().clean()
        try:
            ActionSet(cleaned_data.get('app_name'))
        except Exception:
            raise forms.ValidationError({'app_name': 'Приложения или БД с таким именем не существует'})

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['app_name'].widget.attrs.update({'class': 'form-control'})
