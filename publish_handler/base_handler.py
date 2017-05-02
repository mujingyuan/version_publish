import requests
from tornado.web import RequestHandler
from tornado.concurrent import run_on_executor
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count
from base import ResponseStatus, configuration

class ModuleHander(RequestHandler):
    executor = ThreadPoolExecutor(cpu_count())

    @run_on_executor
    def module_handler(self, payload, is_async=False):
        if is_async:
            module_handler_url = configuration.config_dict['module_handler_url']['async']
        else:
            module_handler_url = configuration.config_dict['module_handler_url']['sync']
        res = requests.post(module_handler_url, json=payload)
        if res and res.status_code == requests.codes.ok:
            context = res.json()
            if context['status'] == ResponseStatus.success.value:
                return True
            else:
                # logger.error("post task: response context status fail, context={}".format(context))
                print(context['message'])
                return False
        else:
            # logger.error("post task: response status fail, response={}".format(res))
            return False
