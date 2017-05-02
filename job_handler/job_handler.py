from tornado.web import RequestHandler
from tornado.escape import json_decode, json_encode
import requests
import json
import uuid
import logging.config
from base.configuration import LOG_SETTINGS
from requests.exceptions import ConnectionError, ConnectTimeout
from base import ResponseStatus, JobStatus, JobCallbackResponseStatus


# curl -XPOST -H "Content-Type: application/json" -d '{"jobid":"201701171136001", "jobname":"offline", "status":"1", "messages": [{"host": "10.10.10.10","status": "1", "message": "anything right"}]}' 10.99.70.73:8100/jobscallback
logging.config.dictConfig(LOG_SETTINGS)
logger = logging.getLogger('job_handler')

class PostPublishJob(object):
    def __init__(self, post_url):
        self.post_url = post_url

    def post_job(self, task):
        payload = task.payload
        res = requests.post(self.post_url, json=payload)
        try:
            context = res.json()
            if res.status_code == requests.codes.ok and context['status'] == ResponseStatus.success:
                return True
            else:
                return False
        except (ConnectionError, ConnectTimeout) as e:
            print(e)


class JobsCallback(RequestHandler):
    def post(self):
        """
        jobid:
        jobname:
        status:
        message: []
        :return:
        """
        body = json_decode(self.request.body)
        job_id = body.get('jobid', '')
        task_id = body.get('taskid', '')
        task_name = body.get('jobname', '')
        task_status = body.get('status', '')
        task_message = body.get('messages', [])
        if job_id == '' or task_name == '' or task_status == '' or task_message == []:
            res = {"status": JobCallbackResponseStatus.fail.value, "message": "some argument is null"}
            logger.error("job callback fail: {}".format("some argument is null"))
            self.write(json.dumps(res))
            self.finish()
        else:
            logger.info('Job_ID: {}, Task_id: {}, Job_Step: {}, Task_Status: {}'.format(job_id, task_id, task_name, task_status))
            zk = self.application.zk
            if zk.update_callback_by_taskid(job_id, task_id, task_status, task_message):
                logger.info("update callback by taskid sucess: jobid={}, taskid={}".format(job_id, task_id))
            else:
                logger.error("update callback by taskid failed: jobid={}, taskid={}".format(job_id, task_id))
            for message in task_message:
                logger.info('"Host": {}, "status": {}, "message": {}'.format(message['host'], message['status'], message['message']))
            if zk.handler_task(job_id, task_id, task_name, task_message, task_status):
                logger.info("handler task success after callback")
                if zk.is_exist_signal(job_id):
                    zk.send_signal(job_id)
                res = {"status": JobCallbackResponseStatus.success.value,
                       "message": "callback receive success, and handler task success after callback"}
            else:
                logger.error("handler task fail after callback")
                res = {"status": JobCallbackResponseStatus.success.value,
                       "message": "callback receive success, but handler task fail after callback"}
            self.write(json_encode(res))
            self.finish()



