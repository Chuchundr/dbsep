import traceback

from psycopg2.errors import InFailedSqlTransaction

from django.shortcuts import render, redirect
from django.contrib.auth.views import LoginView
from django.views.generic import ListView, TemplateView, DetailView, FormView
from django.urls import reverse_lazy
from django.db.utils import OperationalError
from django import forms

from .models import DataBase, ReplicationSlot
from .forms import SeparationForm
from .operations import ActionSet


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


class SeparationView(FormView):
    form_class = SeparationForm
    success_url = reverse_lazy('index')
    template_name = 'database_separator/separate.html'

    def form_valid(self, form):
        if form.is_valid():
            app_name = form.cleaned_data.get('app_name')
            db_name = form.cleaned_data.get('db_name')
            action_set = ActionSet(app_name=app_name, db_name=db_name)
            try:
                action_set.start()
            except Exception:
                tb = traceback.format_exc()
                context = {'error': str(tb)}
                return render(self.request, 'database_separator/fail.html', context)
        return redirect(self.success_url)


class CheckView(TemplateView):
    template_name = 'database_separator/check.html'
