from django.contrib import admin
from django.urls import path, include
from django.views import generic
from django.contrib.staticfiles.urls import staticfiles_urlpatterns
from django.contrib.auth.views import LogoutView
from django.contrib.auth.decorators import login_required

from database_separator.views import MyLoginView, IndexTemplateView

urlpatterns = [
    path('admin/', admin.site.urls),
    path('database_separator/', include('database_separator.urls', namespace='database_separator')),
    path('accounts/login/', MyLoginView.as_view(), name='login'),
    path('accounts/logout/', LogoutView.as_view(), name='logout'),
    path('', login_required(IndexTemplateView.as_view(template_name='index.html')), name='index')
]

urlpatterns += staticfiles_urlpatterns()