import json
import uuid
import threading
import requests
import uuid
import logging.config
from zk_handler.task_operation import ZkOperation
from functools import partial
from kazoo.client import KazooClient
from kazoo.protocol.states import WatchedEvent
from kazoo.recipe.watchers import ChildrenWatch, DataWatch
from kazoo.recipe.lock import Lock, LockTimeout
from portal_handler.release_record import record_target_status
from base import JobStatus, TargetStatus, TaskStatus, ResponseStatus, configuration
from base.configuration import LOG_SETTINGS


logging.config.dictConfig(LOG_SETTINGS)
logger = logging.getLogger('scheduler')


def count_targets(job_targets, statuses):
    return len([x for x in job_targets.values() if x['status'] in statuses])


def choose_target(targets, count):
    c = 0
    for target, data in targets.items():
        if data['status'] == TargetStatus.init.value and c < count:
            c += 1
            yield target


class Scheduler:
    def __init__(self, zk_hosts, zk_root):
        self.zkhandler = ZkOperation(zk_hosts, zk_root)
        self.zk = self.zkhandler.zk
        self.root = zk_root
        self.jobs = set()
        self.event = threading.Event()

    def job_callback(self, job_id, status, messages=None):
        if self.zkhandler.update_callback_by_jobid(job_id, status, messages):
            return True
        else:
            return False

    def target_callback(self, job_id, target, status, messages=None):
        if self.zkhandler.update_callback_by_target(job_id, target, status, messages):
            return True
        else:
            return False

    def task_callback(self, job_id, task_id, status, messages=None):
        if self.zkhandler.update_callback_by_taskid(job_id, task_id, status, messages):
            return True
        else:
            return False

    def get_targets(self, job_id):
        result = {}
        node = '/{}/jobs/{}/targets'.format(self.root, job_id)
        for target in self.zk.get_children(node):
            path = '{}/{}'.format(node, target)
            status, _ = self.zk.get(path)
            result[target] = json.loads(status.decode())
        return result

    def schedule(self, job_id):
        logger.info("schedule start:job id={}".format(job_id))
        node = '/{}/jobs/{}'.format(self.root, job_id)
        data, _ = self.zk.get(node)
        job = json.loads(data.decode())
        """
        {
            "jobid":"",
            "ud": "jpol",
            "module": "mb-inrpc",
            "load_balancing": [{"host":"10.99.70.51"},{"host":"10.99.70.52"}]，
            "targets" : [{"host":"10.99.70.51"},{"host":"10.99.70.52"}], 	// 服务器地址，json数组方式传递
            "parameters" : [{"k1":"v1"},{"k2":"v3"},{"k3":"v3"}],	// 参数，json数组方式传递
            "parallel": 1 //并行数量
            "fail_rate": 0 // 容错数量
        }
        """
        parallel = job.get('parallel', 1)
        fail_rate = job.get('fail_rate', 0)
        #  当前在运行主机状态
        job_targets = self.get_targets(job_id)
        # 如果失败次数大于设定值退出, 暂时取消，执行所有target
        # 并发大于一，失败数可能等于并发数
        # if count_targets(job_targets, (TargetStatus.fail.value,)) > fail_rate:
        #     return self.job_callback(job_id, JobStatus.fail.value)
        # 修改指定target状态为running
        wait_schedule = choose_target(job_targets, parallel - count_targets(job_targets, (TargetStatus.running.value,)))
        for wait_target in wait_schedule:
            self.set_target_status(job_id, wait_target, TargetStatus.running.value)
        self.handle_running_target(job_id)

    def set_target_status(self, job_id, target, status, current_task=None):
        logger.info("set target status:job_id={}, target={}, status={}".format(job_id, target, status))
        node = '{}/jobs/{}/targets/{}'.format(self.root, job_id, target)
        data, _ = self.zk.get(node)
        data = json.loads(data.decode())
        data['status'] = status
        if current_task is not None:
            data['current_task'] = current_task
        tx = self.zk.transaction()
        tx.set_data('{}/jobs/{}/targets/{}'.format(self.root, job_id, target), json.dumps(data).encode())
        tx.commit()
        if not self.target_callback(job_id, target, status):
            logger.error("update callback by target: fail, jobid={}, target={}, status={}".format(job_id, target, status))

    def handle_running_target(self, job_id):
        logger.info("handle_running_target start: job_id={}".format(job_id))
        node = '{}/jobs/{}/targets'.format(self.root, job_id)
        # 这里遍历了job下所有的主机状态，主机数量多的话，要考虑性能问题
        targets = self.zk.get_children(node)
        target_success_count = 0
        target_fail_count = 0
        target_init_count = 0
        target_running_count = 0
        for target in targets:
            target_lock_node = '{}/{}/lock'.format(node, target)
            self.zk.ensure_path(target_lock_node)
            target_lock = Lock(self.zk, target_lock_node)
            try:
                if target_lock.acquire(timeout=1):
                    logger.info("Target Lock acquire: job_id={}, target={}".format(job_id, target))
                    logger.info("handle_running_target start: job_id={}, target={}".format(job_id, target))
                    path = '{}/{}'.format(node, target)
                    target_value, _ = self.zk.get(path)
                    target_value = json.loads(target_value.decode())
                    """
                    target_value = {
                        "status" = 0,
                        "current_task" = "offline"，
                        "next_task" = "stop_service",
                    }
                    """
                    target_status = target_value['status']
                    target_running_task = target_value['current_task']
                    # 处理running的target
                    if target_status == TargetStatus.running.value:
                        self.handle_running_task(job_id, target, target_running_task)
                    elif target_status == TargetStatus.success.value:
                        target_success_count += 1
                    elif target_status == TargetStatus.fail.value:
                        target_fail_count += 1
                    elif target_status == TargetStatus.init.value:
                        target_init_count += 1
                    elif target_status == TargetStatus.running.value:
                        target_running_count += 1
                    else:
                        logger.error("handle running target: unexpected target status, target_status={}".format(target_status))
            except LockTimeout:
                logger.error('Target lock timeout: job_id={}, target={}'.format(job_id, target))
            finally:
                if target_lock.release():
                    logger.info('Target lock release: success, job_id={}, target={}'.format(job_id, target))
                else:
                    logger.error('Target lock release: fail, job_id={}, target={}'.format(job_id, target))
        # job汇总信息
        logger.info("job targets status detail: jobid={}, targets_count={}, target_init_count={}, target_running_count={}, target_success_count={}, target_fail_count={}".format(job_id, len(targets), target_init_count, target_running_count, target_success_count, target_fail_count))
        if (target_success_count + target_fail_count) == len(targets):
            logger.info("job is finished: jobid={}, targets_count={}, target_success_count={}, target_fail_count={}".format(job_id, len(targets), target_success_count, target_fail_count))
            # job 终结点
            if target_success_count == len(targets):
                self.job_callback(job_id, JobStatus.success.value)
            else:
                self.job_callback(job_id, JobStatus.fail.value)
        else:
            logger.info("job is not finished: jobid={},target_count: {}, job target_success_count: {}".format(job_id, len(targets), target_success_count))

    def get_task_status(self, job_id, target, target_running_task):
        node = '{}/jobs/{}/targets/{}/tasks/{}'.format(self.root, job_id, target, target_running_task)
        task_value, _ = self.zk.get(node)
        task_value = task_value.decode()
        return json.loads(task_value)

    def handle_running_task(self, job_id, target, target_running_task):
        logger.info("handle running task start: job_id={}, target={}, target_running_task={}".format(job_id, target, target_running_task))
        node = '{}/jobs/{}/targets/{}/tasks/{}'.format(self.root, job_id, target, target_running_task)
        task_value, _ = self.zk.get(node)
        task_value = json.loads(task_value.decode())
        """
        task_value = {
            "task_name" :"check_version"
            "status" : 0,
            "next_task" : "offline",
            "parameters" : [{"k1":"v1"},{"k2":"v3"},{"k3":"v3"}],
            "callback_url":"url"
        }
        """
        logger.info("running task info: task_info = {}".format(task_value))
        if task_value['status'] == TaskStatus.init.value:
            self.add_new_task(job_id, target, task_value['task_id'])
        elif task_value['status'] == TaskStatus.running.value:
            # 重启后执行正在运行的任务，保留扩展判断
            logger.info("handle running task: task status is running, run this task again")
            self.add_new_task(job_id, target, task_value['task_id'])
        elif task_value['status'] == TaskStatus.success.value:
            if task_value['next_task'] is not None:
                next_task_id = task_value['next_task']
                self.add_new_task(job_id, target, next_task_id)
            else:
                # target执行成功，重新查找新target执行
                self.set_target_status(job_id, target, TargetStatus.success.value, current_task="")
                self.send_signal(job_id)
        elif task_value['status'] == TaskStatus.fail.value:
            # 任务失败 target失败
            self.set_target_status(job_id, target, TargetStatus.fail.value)
            self.send_signal(job_id)
        else:
            logger.error("handle running task: unexpected task status")

    def add_new_task(self, job_id, target, new_task_id):
        logger.info("add new task: job_id={}, target={}, new_task_id={}".format(job_id, target, new_task_id))
        job_info = self.zkhandler.get_job_info(job_id)
        target_info = self.zkhandler.get_target_info(job_id, target)
        task_info = self.zkhandler.get_task_info(job_id, target, new_task_id)
        self.handler_post_task(job_info, target_info, task_info)
        # try:
        #     job_info = self.zkhandler.get_job_info(job_id)
        #     target_info = self.zkhandler.get_target_info(job_id, target)
        #     task_info = self.zkhandler.get_task_info(job_id, target, new_task_id)
        #     self.handler_post_task(job_info, target_info, task_info)
        # except Exception as e:
        #     logger.error('add new task: exception={}'.format(e))

    def handler_post_task(self, job_info, target_info, task_info):
        # 修改新任务状态
        job_id = job_info['jobid']
        target = target_info['target']
        task_id = task_info['task_id']
        tx = self.zk.transaction()
        task_node = '{}/jobs/{}/targets/{}/tasks/{}'.format(self.root, job_id, target, task_id)
        task_info['status'] = TaskStatus.running.value
        tx.set_data(task_node, json.dumps(task_info).encode())

        # 修改主机状态
        target_node = '{}/jobs/{}/targets/{}'.format(self.root, job_id, target)
        target_info['status'] = TargetStatus.running.value
        target_info['current_task'] = task_id
        tx.set_data(target_node, json.dumps(target_info).encode())
        payload = self.get_task_payload(job_info, target_info, task_info)
        if payload:
            tx.commit()
            # 这里有callback阻塞，要改进
            if not self.task_callback(job_id, task_id, TaskStatus.running.value):
                logger.error("update callback by taskid: fail, job_id={}, target={}, task_id={}".format(job_id, target, task_id))
            if self.post_task(payload):
                logger.info("post task: success, jobid={}, target={}, taskid={}".format(job_id, target, task_id))
            else:
                # 发起新任务失败，终止，上报
                logger.error("post task: failed, jobid={}, target={}, taskid=".format(job_id, target, task_id))
                self.set_target_status(job_id, target, TargetStatus.fail.value)
        else:
            logger.error('payload is False:job_id={}, target={}, new_task_id={}'.format(job_id, target, task_id))

    @staticmethod
    def get_task_payload(job_info, target_info, task_info):
        # 解析job info
        job_id = job_info['jobid']
        version = job_info['version']
        build = job_info['build']
        file_list = job_info['file_list']
        parameters = job_info['extend_key']
        load_balancing_hosts = job_info['load_balancing']
        # 解析target info
        target = target_info['target']
        # 解析task info
        task_id = task_info['task_id']
        job_name = task_info['task_name']
        content = task_info['content']
        callback_url = task_info['callback_url']
        parameters.update({"load_balancing": load_balancing_hosts})

        hosts = []
        if isinstance(target, str):
            hosts = [{"host":target}]
        elif isinstance(target, list):
            for host in target:
                hosts.append({"host": host})
        payload = dict()
        # if job_name == 'offline_host':
        #     if load_balancing_hosts:
        #         # offline额外参数, target变为参数, 负载均衡主机作为target
        #         offline_dict = dict()
        #         offline_dict['offline'] = hosts
        #         parameters.append(offline_dict)
        #         payload = {
        #             "jobid": job_id,
        #             "taskid": task_id,
        #             "content": content,
        #             "hosts": load_balancing_hosts,
        #             "jobname": job_name,
        #             "version_info": {
        #                 "version": version,
        #                 "build": build,
        #             },
        #             # 如果传递remotescript则执行remotescript路径脚本，否则按照‘/bu/group/module/jobname ’规则进行拼接脚本路径
        #             "remotescript": "",
        #             "parameters": parameters,
        #             "callback": callback_url,
        #         }
        #     else:
        #         logger.error('load balancing hosts is False: job_id={}, target={}, new_task_id={}'.format(job_id, target, task_id))
        # else:
        payload = {
            "jobid": job_id,
            "taskid": task_id,
            "content": content,
            "hosts": hosts,
            "jobname": job_name,
            "version_info": {
                    "version": version,
                    "build": build,
                    "file_list": file_list,
                },
            "parameters": parameters,
            "callback": callback_url,
        }
        return payload

    def post_job_status(self, job_id):
        logger.info("post job status: job_id={}".format(job_id))

    def send_signal(self, job_id):
        node = '{}/signal/{}'.format(self.root, job_id)
        tx = self.zk.transaction()
        tx.set_data(node, uuid.uuid4().bytes)
        tx.commit()

    def post_task(self, payload):
        res = requests.post(configuration.config_dict['post_task_url'], json=payload)
        if res and res.status_code == requests.codes.ok:
            context = res.json()
            if context['status'] == ResponseStatus.success.value:
                return True
            else:
                logger.error("post task: response context status fail, context={}".format(context))
                return False
        else:
            logger.error("post task: response status fail, response={}".format(res))
            return False

    def handle_new_job(self, jobs):
        """
        处理新的任务
        对新的任务添加监听
        :param jobs:
        :return:
        """
        for job_id in set(jobs).difference(self.jobs):
            logger.info("handler new job: job_id={}".format(job_id))
            self.job_callback(job_id, JobStatus.running.value)
            DataWatch(self.zk, '{}/signal/{}'.format(self.root, job_id),
                      partial(self.handle_exist_job, job_id=job_id))
            if self.zkhandler.is_job_exist(job_id):
                self.schedule(job_id)
            else:
                self.zkhandler.delete_signal(job_id)
                jobs.remove(job_id)
        self.jobs = jobs
        return not self.event.is_set()

    def watch(self):
        """
        监听portal那里插入的新任务
        :return:
        """
        logger.info("scheduler watch start")
        jobs_path = '{}/jobs'.format(self.root)
        if not self.zk.exists(jobs_path):
            self.zk.ensure_path(jobs_path)
        signal_path = '{}/signal'.format(self.root)
        if not self.zk.exists(signal_path):
            self.zk.ensure_path(signal_path)
        ChildrenWatch(self.zk, '{}/signal'.format(self.root), self.handle_new_job)

    def handle_exist_job(self, data, stat, event, job_id):
        """
        收到signal后，进入job处理逻辑， callback结束，监听退出
        """
        logger.info("handle exist job: job_id={}".format(job_id))
        logger.info("{},{},{}".format(data, stat, event))
        if isinstance(event, WatchedEvent) and event.type == 'CHANGED':
            job_callback_value= self.zkhandler.get_callback_info(job_id)
            if job_callback_value:
                job_callback_info = job_callback_value.get("callback_info")
                job_status = job_callback_info["status"]
                if job_status == JobStatus.success.value:
                    logger.info("job success: job_id={}".format(job_id))
                    return False
                elif job_status == JobStatus.fail.value:
                    logger.error("job fail: job_id={}".format(job_id))
                    return False
                self.schedule(job_id)
                return True
            else:
                logger.warning("job callback is not exist: jobid={}".format(job_id))
                logger.info("delete job and signal")
                self.zkhandler.delete_job(job_id)
                self.zkhandler.delete_signal(job_id)
                return False
        else:
            logger.info("handle exist job: WatchedEvent={}".format(event))

    def handle_callback(self, job_id):
        # report_status_to_mysql()
        pass

    def start(self):
        logger.info("scheduler start")
        self.zk.start()
        self.zk.add_auth('digest', 'publish:publish')
        self.watch()
        self.event.wait()
        # while not self.event.is_set():
        #     print("event is not set")
        #     self.event.wait(10)

    def shutdown(self):
        self.event.set()
        self.zk.stop()
        self.zk.close()


if __name__== '__main__':
    zk_connect = '127.0.0.1:2181'
    zk_root = '/version_publish'
    scheduler = Scheduler(zk_connect, zk_root)
    try:
        scheduler.start()
    except KeyboardInterrupt:
        scheduler.shutdown()
