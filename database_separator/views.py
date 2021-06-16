from django.shortcuts import render
from django.contrib.auth.views import LoginView
from django.views.generic import ListView, TemplateView, DetailView
from django.urls import reverse_lazy

from .models import DataBase, ReplicationSlot


class MyLoginView(LoginView):

    def get_success_url(self):
        return reverse_lazy('index')


class IndexTemplateView(TemplateView):

    def get_context_data(self):
        context = super().get_context_data()
        context['db_list'] = DataBase.objects.all()
        context['slot_list'] = ReplicationSlot.objects.all().order_by('active')
        return context


class DataBaseDetailView(DetailView):
    model = DataBase
    template_name = 'database_separator/database_detail.html'