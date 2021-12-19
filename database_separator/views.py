import traceback

from django.shortcuts import render, redirect
from django.contrib.auth.views import LoginView
from django.core.management import call_command
from django.views.generic import ListView, TemplateView, DetailView, FormView
from django.http import HttpResponseRedirect
from django.urls import reverse_lazy

from .models import DataBase, ReplicationSlot, CheckSum
from .forms import SeparationForm
from .scripts.separation import ActionSet, BackupActionSet
from .scripts.relaunch import ActionSetForRelaunch


class MyLoginView(LoginView):
    """
    Вьюшка для авторизации
    """
    def get_success_url(self):
        return reverse_lazy('index')


class IndexTemplateView(TemplateView):
    """
    Вьюшка для главной страницы
    контекст:
        db_list - список БД
        slot_list - список слотов
    """
    def get_context_data(self):
        context = super().get_context_data()
        context['db_list'] = DataBase.objects.all()
        context['slot_list'] = ReplicationSlot.objects.all().order_by('active')
        return context


class DataBaseDetailView(DetailView):
    """
    Детализация сущностей в БД
    """
    model = DataBase
    template_name = 'database_separator/database_detail.html'


class SeparationView(FormView):
    """
    Вьюшка для разделения
    """
    form_class = SeparationForm
    success_url = reverse_lazy('index')
    template_name = 'database_separator/separate.html'

    def form_valid(self, form):
        if form.is_valid():
            app_name = form.cleaned_data.get('app_name')
            db_name = form.cleaned_data.get('db_name')
            action_set = ActionSet(app_name=app_name, db_name=db_name)
            try:
                action_set.execute_script()  # запускаем сценарий разделения
            except Exception:  # при возникновении ошибки
                backup_action_set = BackupActionSet(app_name=app_name, db_name=db_name)
                backup_action_set.backup()  # удаляем все сущности из БД, если есть
                tb = traceback.format_exc()  # выводим трейсбэк на страницу
                context = {'error': str(tb)}
                return render(self.request, 'database_separator/fail.html', context)
        return redirect(self.success_url)


class CheckView(TemplateView):
    """
    Вспомогательная вьюшка для проверки выполнения предварительных действий
    """
    template_name = 'database_separator/check.html'


class AdditionalFunctionalityView(TemplateView):
    """
    Вьюшка для дополнительных функций
    """
    template_name = 'database_separator/additional.html'


class CheckSumsView(ListView):
    """
    Вьюшка для отображения контрольных сумм
    """
    model = CheckSum
    paginate_by = 10

    def get_queryset(self):
        return CheckSum.objects.all()


def initialize(request):
    call_command('initialize')
    return HttpResponseRedirect(reverse_lazy('index'))


def replication_relaunch(request):
    action_set = ActionSetForRelaunch()
    action_set.execute_script()
    return HttpResponseRedirect(reverse_lazy('index'))