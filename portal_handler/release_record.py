def record_target_status(jobid, target, status):
    return True

import requests
from base import CallbackLevel, CallbackResponseStatus
from base.configuration import LOG_SETTINGS
from base import TargetStatus
import logging.config


logging.config.dictConfig(LOG_SETTINGS)
logger = logging.getLogger('release_record')


class PortalCallback(object):
    def __init__(self, job_id, job_info):
        self.job_id = job_id
        self.job_callback_info = job_info
        self.job_callback_info["targets"] = dict()

    def _get_job_callback(self):
        return self.job_callback_info["callback"]["callback_job_url"]

    def _get_target_callback(self):
        return self.job_callback_info["callback"]["callback_target_url"]

    @staticmethod
    def _post_callback(post_url, payload):
        res = requests.post(post_url, json=payload)
        if res and res.status_code == requests.codes.ok:
            context = res.json()
            if context['status'] == CallbackResponseStatus.success.value:
                logger.info("post callback: response context status success, context={}".format(context))
                # data_code = context.get("data", 0)
                # if data_code == 100:
                #     do something with data_code == 100
                return True
            else:
                logger.error("post callback: response context status fail, context={}, payload={}".format(context, payload))
                return False
        else:
            logger.error("post callback: response status fail, response={}".format(res))
            return False

    def _job_callback(self, callback_info):
        logger.info("job callback, callback_info={}".format(callback_info))
        job_callback_url = self._get_job_callback()
        payload = {
            "job_id": callback_info["job_id"],
            "status": callback_info["status"],
            "messages": callback_info["messages"]
        }
        if self._post_callback(job_callback_url, payload):
            return True
        else:
            return False

    def _target_callback(self, callback_info):
        logger.info("target callback, callback_info={}".format(callback_info))
        target_callback_url = self._get_target_callback()
        payload = {
            "job_id": callback_info["job_id"],
            "target": callback_info["target"],
            "status": callback_info["status"],
            "messages": callback_info["messages"]
        }
        if self._post_callback(target_callback_url, payload):
            return True
        else:
            return False

    def _task_callback(self, callback_info):
        logger.info("task callback, callback_info={}".format(callback_info))
        task_callback_url = self._get_target_callback()
        # 'messages': [
        # {
        #     'message':
        #         {
        #           'failed': ['1489552508,1. DOWNLOAD VERSION,failed'],
        #           'unreachable': [],
        #           'success': ['1489552508,0 MAKE DIR,success']
        #         },
        #     'status': 2,
        #     'host': '10.99.70.75'
        # }
        # ]
        messages = callback_info.get("messages", [])
        callback_messages = []
        #  这里感觉很不好。。
        if messages:
            if isinstance(messages, list):
                for message in messages:
                    message_detail = message.get('message', dict())
                    if isinstance(message_detail, dict):
                        for msg in message_detail.values():
                            if msg:
                                if isinstance(msg, list):
                                    callback_messages.extend(msg)
                                elif isinstance(msg, str):
                                    callback_messages.append(msg)
                    elif isinstance(message, str):
                        callback_messages.append(messages)
            else:
                logger.error("callback message is not list, messages = {}".format(messages))
        else:
            callback_messages = []
        payload = {
            "job_id": callback_info["job_id"],
            "target": callback_info["target"],
            "status": callback_info["status"],
            "messages": callback_messages
        }
        if self._post_callback(task_callback_url, payload):
            return True
        else:
            return False

    def _update_job(self, status, messages=None):
        self.job_callback_info["status"] = status
        if messages is not None:
            self.job_callback_info["messages"] = messages

    def _update_job_target(self, target, status, messages=None):
        self.job_callback_info.setdefault("targets", dict())
        target_callback_info = self.job_callback_info["targets"].get(target, dict())
        if target_callback_info:
            target_callback_info["status"] = status
            if messages is not None:
                target_callback_info["message"] = messages
        else:
            target_callback_info = {
                "status": status,
                "messages": messages,
                "steps": dict()
            }
        self.job_callback_info["targets"][target] = target_callback_info

    def _update_job_task(self, target, task_name, status, messages=None):
        self.job_callback_info.setdefault("targets", dict())
        if target not in self.job_callback_info["targets"]:
            self._update_job_target(target, TargetStatus.running.value, "")
        target_callback_info = self.job_callback_info["targets"].get(target, dict())
        if target_callback_info:
            target_callback_info.setdefault("tasks", dict())
            task_callback_info = target_callback_info["tasks"].get(task_name, dict())
            if task_callback_info:
                task_callback_info["status"] = status
                if messages is not None:
                    task_callback_info["messages"] = messages
            else:
                task_callback_info = {
                    "status": status,
                    "messages": messages
                }
            self.job_callback_info["targets"][target][task_name] = task_callback_info
        else:
            logger.error("job callback info: {}".format(self.job_callback_info))
            logger.error("update job task failed: target is not exist, job_id={}, target={}, task_name={}".format(self.job_id, target, task_name))

    def post_callback(self, callback_value):
        callback_level = callback_value["callback_level"]
        callback_info = callback_value["callback_info"]
        status = callback_info["status"]
        messages = callback_info.get("messages", None)
        if callback_level == CallbackLevel.job.value:
            self._update_job(status, messages)
            if self._job_callback(callback_info):
                return True
        elif callback_level == CallbackLevel.target.value:
            target = callback_info['target']
            self._update_job_target(target, status, messages)
            if self._target_callback(callback_info):
                return True
        elif callback_level == CallbackLevel.task.value:
            target = callback_info['target']
            task_name = callback_info["task_name"]
            self._update_job_task(target, task_name, status, messages)
            if self._task_callback(callback_info):
                return True
        else:
            logger.error("post callback:fail, callback_level={}".format(callback_level))
