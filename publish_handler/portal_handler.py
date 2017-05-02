from tornado.web import RequestHandler
from tornado.escape import json_decode, json_encode
import json
import copy
import uuid
import logging.config
from base.configuration import get_task_id_prefix, config_dict, LOG_SETTINGS
from base import TargetStatus, TaskStatus, ResponseStatus

logging.config.dictConfig(LOG_SETTINGS)
logger = logging.getLogger('portal_handler')

# curl -XPOST -H "Content-Type: application/json" -d '{"job_id":"jobid_20170207173800", "environment":"test", "bu":"jpol", "module": "mb-inrpc", "hosts":["10.99.70.61"], "version":"v1.6.16", "build": 48}' 10.99.70.73:8100/publish_version
class PublishHandler(RequestHandler):
    def post(self, *args, **kwargs):
        body = json_decode(self.request.body)
        # body
        job_id = body.get('job_id', '')
        release_config = body.get('release_config', '')
        job_type = body.get('job_type', '')
        job_info = body.get('job_info', '')
        targets = body.get('targets', [])
        parameters = body.get('parameters', '')
        callback_url = body.get('callback', '')
        if not job_id or not release_config or not job_type or not job_info or not targets or not callback_url:
            res = {
                    "status": ResponseStatus.fail.value,
                    "message": "argument is null"
                }
            self.write(json.dumps(res))
            self.finish()

        # release config
        environment = release_config.get('environment', '')
        project = release_config.get('project', '')
        module = release_config.get('module', '')
        extend_key = release_config.get('extend_key', dict())

        # version info
        version = job_info.get('version', '')
        build = job_info.get('build', '')
        file_list = job_info.get('filelist')
        # targets
        load_balancing = targets.get('load_balancing', [])
        execute_hosts = targets.get('execute_hosts', [])
        # parameters
        parallel = parameters.get('parallel', 1)
        fail_rate = parameters.get('fail_rate', 0)
        job_sequence = body.get('job_sequence', config_dict['publish_task_sequence'])
        #create job value
        job_value = {
                    "jobid": job_id,
                    "job_type": job_type,
                    "environment": environment,
                    "project": project,
                    "module": module,
                    "version": version,
                    "build": build,
                    "file_list": file_list,
                    "load_balancing": load_balancing,
                    "targets": execute_hosts,
                    "extend_key": extend_key,
                    "parallel": parallel,
                    "fail_rate": fail_rate,
                    # portal callback url
                    "callback": callback_url,
                    # task callback url
                    "callback_url": config_dict['task_callback_url']
                }

        ret = self.add_task_to_scheduler(job_id, job_value, execute_hosts, job_sequence)
        if ret:
            if self.start_job(job_id):
                res = {
                    "status": ResponseStatus.success.value,
                    "message": "job receive success"
                }
                self.write(json.dumps(res))
                self.finish()
        else:
            res = {
                    "status": ResponseStatus.fail.value,
                    "message": "job create fail"
                }
            self.write(json.dumps(res))
            self.finish()

    @staticmethod
    def get_task_id(task_name):
        prefix = get_task_id_prefix()
        return prefix + task_name + '_' + uuid.uuid4().hex

    def add_task_to_scheduler(self, job_id, job_value, hosts, job_sequence):
        logger.info("add task to scheduler: job_id={}".format(job_id))
        zk = self.application.zk
        sequence = copy.deepcopy(job_sequence)

        if not zk.is_job_exist(job_id):
            ret = zk.create_new_job(job_id, job_value)
            if ret:
                if zk.init_callback_by_jobid(job_id):
                    logger.info("init callback by jobid success: jobid={}".format(job_id))
                else:
                    logger.error("init callback by jobid fail: jobid={}".format(job_id))
                    return False
                for host in hosts:
                    """
                    {
                    "task_id": 0,
                    "status": 0,
                    "task_name": "transfer_version",
                    "parameters": [],
                    "callback_url": "",
                    "next_task": "offline_host",
                    },
                    """
                    first_task_name = sequence[0]
                    first_task_id = self.get_task_id(first_task_name)
                    target_value = {
                        "target": host,
                        "status": TargetStatus.init.value,
                        "current_task": first_task_id
                    }
                    ret = zk.create_new_target(job_id, host, target_value)
                    if ret:
                        if zk.init_callback_by_target(job_id, host):
                            logger.info("init callback by target success: jobid={}, target={}".format(job_id, host))
                        else:
                            logger.info("init callback by target fail: jobid={}, target={}".format(job_id, host))
                        # 列表生成任务单链执行顺序
                        sequence_index = 0
                        ret = False
                        next_task_id = ''
                        while sequence_index < len(sequence) and next_task_id is not None:
                            task = dict()
                            task_name = sequence[sequence_index]
                            if sequence_index < len(sequence) - 1:
                                next_task_name = sequence[sequence_index + 1]
                            else:
                                next_task_name = ''
                            # 生成任务参数
                            if sequence_index == 0 :
                                task['task_id'] = first_task_id
                            else:
                                task['task_id'] = next_task_id
                            task['task_name'] = task_name
                            task['status'] = TaskStatus.init.value
                            task['callback_url'] = job_value['callback_url']
                            task['content'] = {"environment": job_value["environment"], "project": job_value["project"], "module": job_value["module"]}
                            task['remotescript'] = job_value.get("remotescript", "")
                            if next_task_name == '':
                                next_task_id = None
                            else:
                                next_task_id = self.get_task_id(next_task_name)
                            task['next_task'] = next_task_id
                            sequence_index += 1
                            ret = zk.create_new_task(job_id, host, task)
                            if ret:
                                if zk.init_callback_by_taskid(job_id, host, task['task_id'], task['task_name']):
                                    logger.info("init callback by taskid success: jobid={}, taskid={}".format(job_id, task['task_id']))
                                else:
                                    logger.error("init callback by taskid fail: jobid={}, taskid={}".format(job_id, task['task_id']))
                            else:
                                logger.error("add task to schduler fail: job_id={}, host={}, task={}, create task failed".format(job_id, host, task))
                                return ret
                    else:
                        logger.error("add task to scheduler fail: job_id={}, host={}, create target failed".format(job_id, host))
                return True
            else:
                logger.error("add task to scheduler fail: job_id={} is created fail".format(job_id))
                return False
        else:
            logger.error("add task to scheduler fail: job_id={} is exist".format(job_id))
            return False

    def start_job(self, job_id):
        ret = self.application.zk.create_job_signal(job_id)
        return ret




