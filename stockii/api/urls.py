from django.conf.urls import patterns, include, url
from django.contrib import admin

urlpatterns = patterns('api.views',
    # Examples:
    # url(r'^$', 'stockii.views.home', name='home'),
    # url(r'^blog/', include('blog.urls')),
    url(r'^(?P<api>\w+)$', 'processRequest'),
)
