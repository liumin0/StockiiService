from django.shortcuts import render
from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.renderers import JSONRenderer
from django.http import HttpResponse
import sys, os, imp

__CURDIR__ = os.path.dirname(os.path.abspath(__file__))
class JSONResponse(HttpResponse):
    """
    An HttpResponse that renders it's content into JSON.
    """
    def __init__(self, data, **kwargs):
        content = JSONRenderer().render(data)
        kwargs['content_type'] = 'application/json'
        super(JSONResponse, self).__init__(content, **kwargs)
        
def importModule(name):
    if name in sys.modules:
        return sys.modules[name]
    modulePath = os.path.join(__CURDIR__, 'services', name, '__init__.py')
    ret = None
    try:
        scheme = imp.load_source(name, modulePath)
        ret = scheme
    except:
        pass
    return ret
        
# Create your views here.
@api_view(['GET', 'POST'])
def processRequest(request, api):
    """
    Deal with a request.
    """  

    print request.method
    print request.GET
    print request.POST
    apiName = api
    print 'API=',apiName
    if request.method == 'GET':
        args = request.GET
    else:
        args = request.POST
        
    module = importModule(apiName)
    if module is not None:
        try:
            success, ret = module.run(args)
        except:
            return JSONResponse('Error occured')
        return JSONResponse(ret)
       
    return JSONResponse('No api', status=status.HTTP_404_NOT_FOUND)
