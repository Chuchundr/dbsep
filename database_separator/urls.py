from django.urls import path

from .views import DataBaseDetailView, SeparationView, CheckView


app_name = 'database_separator'

urlpatterns = [
    path('database/<int:pk>', DataBaseDetailView.as_view(), name='db'),
    path('separate', SeparationView.as_view(), name='separate'),
    path('check/', CheckView.as_view(), name='check')
]