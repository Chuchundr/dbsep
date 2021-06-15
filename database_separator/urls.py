from django.urls import path

from .views import DataBaseDetailView


app_name = 'database_separator'

urlpatterns = [
    path('database/<int:pk>', DataBaseDetailView.as_view(), name='db')
]