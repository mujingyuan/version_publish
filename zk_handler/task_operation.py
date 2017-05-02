import json
import threading
import uuid
import logging.config
from functools import partial
from kazoo.client import KazooClient
from kazoo.recipe.watchers import ChildrenWatch, DataWatch
from base import TaskStatus, JobStatus, TargetStatus, CallbackLevel
from base.configuration import LOG_SETTINGS

logging.config.dictConfig(LOG_SETTINGS)
logger = logging.getLogger('zkhandler')

class ZkOperation(object):
    def __init__(self, zk_hosts, zk_root):
        self.zk = KazooClient(zk_hosts)
        self.root = zk_root
        self.tasks = set()
        self.event = threading.Event()

    def start(self):
        if self.zk.exists:
            self.zk.start()
            self.zk.add_auth('digest', 'publish:publish')
        if self.zk.connected:
            self.zk.ensure_path(self.root)

    def is_job_exist(self, job_id):
        if job_id == '':
            raise Exception('job_id is ""')
        node = self.root + '/jobs/' + job_id
        return self.zk.exists(node)

    def check_task_status(self, path):
        if path == '':
            raise Exception('path is ""')
        node = self.root + path
        data, _ = self.zk.get(node)
        return data['Status']

    def _is_exist(self, node):
        if self.zk.connected and self.zk.exists(node):
            if self.zk.exists(node):
                return True
            else:
                return False
        else:
            logger.error('zk not connected or node is exists')
            return False

    def _create_node(self, node, value=None):
        if value is None:
            value = ''
        value = json.dumps(value)
        if self.zk.connected and not self.zk.exists(node):
            self.zk.create(node, makepath=True, value=value.encode())
            return True
        else:
            logger.error('zk not connected or node is exists')
            return False

    def _update_node(self, node, value):
        if self.zk.connected and self.zk.exists(node):
            tx = self.zk.transaction()
            tx.set_data(node, json.dumps(value).encode())
            tx.commit()
            return True
        else:
            logger.error('update node failed: zk not connected or node is not exists')
            return False

    def _get_node(self, node):
        if self.zk.connected and self.zk.exists(node):
            node_value, _ = self.zk.get(node)
            if node_value:
                return json.loads(node_value.decode())
            else:
                return {}
        else:
            logger.error('zk not connected or node is exists')
            return False

    def _delete_node(self, node):
        if self.zk.connected:
            if not self.zk.exists(node):
                return True
            else:
                self.zk.delete(node, recursive=True)
                return True
        else:
            logger.error('zk not connected')
            return False

    # is exist
    def is_exist_signal(self, job_id):
        node = '/{}/signal/{}'.format(self.root, job_id)
        return self._is_exist(node)

    # CREATE
    def create_new_job(self, job_id, job_value=None):
        if job_value is None:
            job_value = ''
        if job_id != '':
            node = self.root + '/jobs/' + job_id
            ret = self._create_node(node, job_value)
            return ret
        else:
            logger.error('job_id is null')
            return False

    def create_new_target(self, job_id, target, target_value):
        node = '/{}/jobs/{}/targets/{}'.format(self.root, job_id, target)
        ret = self._create_node(node, target_value)
        return ret

    def create_new_task(self, job_id, target, task):
        node = '/{}/jobs/{}/targets/{}/tasks/{}'.format(self.root, job_id, target, task['task_id'])
        ret = self._create_node(node, task)
        return ret

    def create_job_signal(self, job_id):
        node = '/{}/signal/{}'.format(self.root, job_id)
        ret = self._create_node(node, uuid.uuid4().hex)
        return ret

    # GET
    def get_job_info(self, job_id):
        job_node = '{}/jobs/{}'.format(self.root, job_id)
        job_value, _ = self.zk.get(job_node)
        job_info = json.loads(job_value.decode())
        return job_info

    def get_target_info(self, job_id, target):
        target_node = '{}/jobs/{}/targets/{}'.format(self.root, job_id, target)
        target_value, _ = self.zk.get(target_node)
        target_info = json.loads(target_value.decode())
        return target_info

    def get_task_info(self, job_id, target, task_id):
        task_node = '{}/jobs/{}/targets/{}/tasks/{}'.format(self.root, job_id, target, task_id)
        task_value, _ = self.zk.get(task_node)
        task_info = json.loads(task_value.decode())
        return task_info

    # UPDATE
    def update_job_status(self, job_id, task):
        if job_id != '' and task is not None:
            node = self.root + '/signal/' + job_id
        else:
            raise Exception('job_id is ""')
        if self.zk.connected and self.is_job_exist(job_id):
            tx = self.zk.transaction()
            tx.set_data(node, task.encode())
            tx.commit()

    def handler_task(self, job_id, task_id, task_name, task_message, status):
        # 为不必传回target, 遍历任务节点
        if not self.is_job_exist(job_id):
            logger.error("can not find this jobid: {}".format(job_id))
            return False
        job_node = "{}/jobs/{}/targets".format(self.root, job_id)
        for target in self.zk.get_children(job_node):
            target_node = "{}/{}/tasks".format(job_node, target)
            for task in self.zk.get_children(target_node):
                if task == task_id:
                    task_node = "{}/{}".format(target_node, task)
                    task_value, _ = self.zk.get(task_node)
                    new_task_value = json.loads(task_value.decode())
                    new_task_value['status'] = status
                    tx = self.zk.transaction()
                    tx.set_data(task_node, json.dumps(new_task_value).encode())
                    tx.commit()
                    task_value, _ = self.zk.get(task_node)
                    return True
        logger.error("can not find this taskid: {} in {}".format(task_id, job_id))
        return False

    def get_target_by_taskid(self, job_id, task_id):
        if self.is_job_exist(job_id):
            node = "{}/jobs/{}/targets".format(self.root, job_id)
            for target in self.zk.get_children(node):
                path = '{}/{}/tasks'.format(node, target)
                for taskid in self.zk.get_children(path):
                    if taskid == task_id:
                        return target
            return False
        else:
            logger.error("job is not exist: job_id={}".format(job_id))

    def send_signal(self, job_id):
        node = '{}/signal/{}'.format(self.root, job_id)
        logger.info("send singal: {}".format(job_id))
        tx = self.zk.transaction()
        tx.set_data(node, uuid.uuid4().bytes)
        tx.commit()

    # DELETE
    def delete_job(self, job_id):
        node = "{}/jobs/{}".format(self.root, job_id)
        logger.info("delete job: job_id={}".format(job_id))
        self._delete_node(node)

    def delete_signal(self, job_id):
        node = '{}/signal/{}'.format(self.root, job_id)
        logger.info("delete singal: {}".format(job_id))
        self._delete_node(node)

    def delete_target(self, job_id, target):
        target_node = '{}/jobs/{}/targets/{}'.format(self.root, job_id, target)
        logger.info("delete target: job_id={}, target={}".format(job_id, target))
        self._delete_node(target_node)

    def delete_task(self, job_id, target, task_id):
        task_node = '{}/jobs/{}/targets/{}/tasks/{}'.format(self.root, job_id, target, task_id)
        logger.info("delete task: job_id ={}, target={}, task_id={}".format(job_id, target, task_id))
        self._delete_node(task_node)

#################################
    # CALLBACK
    ## exsit CALLBACK
    def is_exist_callback(self, callback_node):
        node = "{}/callback/{}".format(self.root, callback_node)
        if self.zk.exists(node):
            return True
        else:
            return False

    ## INIT CALLBACK
    def init_callback_by_jobid(self, job_id):
        node = "{}/callback/{}".format(self.root, job_id)
        job_callback_value = {
            "job_id": job_id,
            "status": JobStatus.init.value,
            "messages": ""
        }
        callback = {
            "callback_level": CallbackLevel.job.value,
            "callback_info": job_callback_value
        }
        ret = self._create_node(node, callback)
        return ret

    def init_callback_by_target(self, job_id, target):
        node = "{}/callback/{}".format(self.root, job_id + "_" + target)
        target_callback_value = {
            "job_id": job_id,
            "target": target,
            "status": TargetStatus.init.value,
            "messages": ""
        }
        callback = {
            "callback_level": CallbackLevel.target.value,
            "callback_info": target_callback_value
        }
        ret = self._create_node(node, callback)
        return ret

    def init_callback_by_taskid(self, job_id, target, task_id, task_name):
        node = "{}/callback/{}".format(self.root, task_id)
        taskid_callback_value = {
            "job_id": job_id,
            "target": target,
            "task_name": task_name,
            "status": JobStatus.init.value,
            "messages": "",
        }
        callback = {
            "callback_level": CallbackLevel.task.value,
            "callback_info": taskid_callback_value
        }
        ret = self._create_node(node, callback)
        return ret

    ## GET CALLBACK
    def get_callback_info(self, callback):
        node = "{}/callback/{}".format(self.root, callback)
        if self.zk.exists(node):
            node_value = self._get_node(node)
            return node_value
        else:
            return False

    ## UPDATE CALLBACK
    def update_callback_by_jobid(self, job_id, status, messages=None):
        node = "{}/callback/{}".format(self.root, job_id)
        if not self.zk.exists(node):
            return False
        node_value = self._get_node(node)
        node_value["callback_info"]["status"] = status
        if messages is not None:
            node_value["callback_info"]["messages"] = messages
        ret = self._update_node(node, node_value)
        return ret

    def update_callback_by_target(self, job_id, target, status, messages=None):
        node = "{}/callback/{}".format(self.root, job_id + "_" + target)
        if not self.zk.exists(node):
            return False
        node_value = self._get_node(node)
        node_value["callback_info"]["status"] = status
        if messages is not None:
            node_value["callback_info"]["messages"] = messages
        ret = self._update_node(node, node_value)
        return ret

    def update_callback_by_taskid(self, job_id, taskid, status, messages=None):
        node = "{}/callback/{}".format(self.root, taskid)
        if not self.zk.exists(node):
            return False
        node_value = self._get_node(node)
        node_value["callback_info"]["status"] = status
        if messages is not None:
            node_value["callback_info"]["messages"] = messages
        ret = self._update_node(node, node_value)
        return ret

    ## DELETE CALLBACK
    def delete_callback_node(self, callback):
        node = "{}/callback/{}".format(self.root, callback)
        ret = self._delete_node(node)
        if ret:
            logger.info("delete callback node success: callback={}".format(node))
        else:
            logger.error("delete callback node fail: callback={}".format(node))
        return ret

