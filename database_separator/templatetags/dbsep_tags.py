from django import template
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger

register = template.Library()


def get_paginate_obj(request, qs, page_len=10):
    paginator = Paginator(qs, page_len)
    page = request.GET.get('page', 1)
    try:
        page_obj = paginator.page(page)
    except PageNotAnInteger:
        page_obj = paginator.page(page_len)
    except EmptyPage:
        page_obj = paginator.page(paginator.num_pages)
    return page_obj