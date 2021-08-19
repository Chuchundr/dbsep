from django.urls import path
from django.contrib.auth.decorators import login_required

from .views import DataBaseDetailView, SeparationView, CheckView, initialize


app_name = 'database_separator'

urlpatterns = [
    path('database/<int:pk>', login_required(DataBaseDetailView.as_view()), name='db'),
    path('separate', login_required(SeparationView.as_view()), name='separate'),
    path('check/', login_required(CheckView.as_view()), name='check'),
    path('initialize', login_required(initialize), name='initialize')
]