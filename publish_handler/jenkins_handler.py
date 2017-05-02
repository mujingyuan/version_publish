import logging
import json
from tornado.web import asynchronous
from tornado.gen import coroutine

from base.configuration import LOG_SETTINGS, common_config_dict
from publish_handler.base_handler import ModuleHander

logging.config.dictConfig(LOG_SETTINGS)
logger = logging.getLogger('jenkins_handler')


class JenkinsHandler(ModuleHander):

    @asynchronous
    @coroutine
    def post(self, *args, **kwargs):
        body = self.request.body.decode()
        body = json.loads(body)
        if self.check_body(body):
            default_ansible_parameters = common_config_dict["default_ansible_parameters"]
            parameters = body.get("parameters", default_ansible_parameters)
            job_name = body.get("job_name", default_ansible_parameters.get("job_name"))
            payload = self.get_payload(body, job_name, parameters)
            is_async = parameters.get("is_async")
            res = yield self.module_handler(payload, is_async=is_async)
            if res:
                self.write("module hander run success")
                self.finish()
            else:
                logger.error("module hander run fail")
                self.write("module hander run fail")
                self.finish()
        else:
            self.write("some argument is null")
            self.finish()

    @staticmethod
    def get_payload(body, job_name, parameters):
        content = {
            "environment": body.get("environment"),
            "project": body.get("project"),
            "module": body.get("module")
        }
        payload = {
            "content": content,
            "jobname": job_name,
            "version_info": {
                    "version": body.get("version"),
                    "build": body.get("build"),
                    "file_list": body.get("file_list"),
                },
            "extend_key": body.get("extend_key"),
            "parameters": parameters
        }
        return payload

    def check_body(self, body):
        try:
            if body['environment'] == '' or body['project'] == '' or body['module'] == '' \
                    or body['version'] == '' or body['build'] == '' or body['file_list'] == '':
                logger.error("some argument is null")
                return False
            else:
                return True
        except KeyError:
            self.write('some argument miss')
            self.finish()

