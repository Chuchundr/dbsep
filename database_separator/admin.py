from django.contrib import admin
from .models import DataBase, Publication, Subscription, ReplicationSlot, PubTables


class TablesTabularInline(admin.TabularInline):
    model = PubTables
    extra = 0


@admin.register(DataBase)
class DataBaseAdmin(admin.ModelAdmin):
    list_display = ['name', ]


@admin.register(Publication)
class PublicationAdmin(admin.ModelAdmin):
    inlines = [TablesTabularInline, ]
    list_display = ['name', ]


@admin.register(Subscription)
class SubscriptionAdmin(admin.ModelAdmin):
    list_display = ['name', ]


@admin.register(ReplicationSlot)
class ReplicationSlotAdmin(admin.ModelAdmin):
    list_display = ['name', ]