import json
import logging.config
import requests
import threading
from base.configuration import LOG_SETTINGS
from functools import partial
from kazoo.client import KazooClient
from kazoo.recipe.watchers import ChildrenWatch, DataWatch
from kazoo.protocol.states import WatchedEvent
from zk_handler.task_operation import ZkOperation
from portal_handler.release_record import PortalCallback
from base import CallbackLevel, JobStatus, TargetStatus, TaskStatus

logging.config.dictConfig(LOG_SETTINGS)
logger = logging.getLogger('callback')


class Callback:
    def __init__(self, zk_hosts, zk_root):
        self.zkhandler = ZkOperation(zk_hosts, zk_root)
        self.zk = self.zkhandler.zk
        self.root = zk_root
        self.event = threading.Event()
        self.callbacks = {}
        self.callback_handlers = dict()

    def get_callback_value(self, callback):
        callback_value = self.zkhandler.get_callback_info(callback)
        return callback_value

    def delete_callback(self, callback):
        return self.zkhandler.delete_callback_node(callback)

    def delete_job(self, job_id):
        self.zkhandler.delete_job(job_id)
        self.zkhandler.delete_signal(job_id)

    def delete_target(self, job_id, target):
        self.zkhandler.delete_target(job_id, target)

    def delete_task(self, job_id, target, task_id):
        self.zkhandler.delete_task(job_id, target, task_id)

    def run_callback(self, callback):
        callback_value = self.get_callback_value(callback)
        callback_info = callback_value["callback_info"]
        job_id = callback_info["job_id"]
        if not self.zkhandler.is_exist_callback(job_id):
            # job 已返回， callback状态还是init or running,删除callback
            logger.warning('job callback is not exsit')
            if callback_info["status"] in (TaskStatus.init.value, TaskStatus.running.value):
                self.delete_callback(callback)
                return
        if job_id in self.callback_handlers.keys():
            callback_handler = self.callback_handlers.get(job_id)
        else:
            job_info = self.zkhandler.get_job_info(job_id)
            callback_handler = PortalCallback(job_id, job_info)
            self.callback_handlers[job_id] = callback_handler
        if callback_handler.post_callback(callback_value):
            callback_level = callback_value["callback_level"]
            callback_status = callback_info["status"]
            if callback_level == CallbackLevel.job.value:
                if callback_status in (JobStatus.success.value, JobStatus.fail.value):
                    logger.info("run callback: job is finished, callback status ={}".format(callback_status))
                    if self.delete_callback(callback):
                        self.delete_job(job_id)
                        del self.callback_handlers[job_id]
                    else:
                        logger.error("delete callback failed: callback={}".format(callback))
            elif callback_level == CallbackLevel.target.value:
                if callback_status in (TargetStatus.success.value, TargetStatus.fail.value):
                    logger.info("run callback: target is finished, callback status ={}".format(callback_status))
                    self.delete_callback(callback)
            elif callback_level == CallbackLevel.task.value:
                if callback_status in (TaskStatus.success.value, TaskStatus.fail.value):
                    logger.info("run callback: task is finished, callback status ={}".format(callback_status))
                    self.delete_callback(callback)
            else:
                logger.error("wrong callback callback level")
        else:
            logger.error("post callback error: callback_value={}".format(callback_value))

    def watch(self):
        callback_path = '{}/callback'.format(self.root)
        if not self.zk.exists(callback_path):
            self.zk.ensure_path(callback_path)
        ChildrenWatch(self.zk, '/{}/callback'.format(self.root), self.handle_new_callback)

    def compensate(self):
        while not self.event.is_set():
            logger.info("starting compensate")
            for callback in self.zk.get_children('/{}/callback'.format(self.root)):
                self.run_callback(callback)
            self.event.wait(180)

    def handle_new_callback(self, callbacks):
        for callback in set(callbacks).difference(self.callbacks):
            logger.info("handler new callback: callback={}".format(callback))
            DataWatch(self.zk, '{}/callback/{}'.format(self.root, callback),
                      partial(self.handle_exist_callback, callback=callback))
            self.run_callback(callback)
        self.callbacks = callbacks
        return not self.event.is_set()

    def handle_exist_callback(self, data, stat, event, callback):
        if isinstance(event, WatchedEvent):
            if event.type == 'CHANGED':
                logger.info("callback changed: WatchedEvent_Type={}, WatchedEvent_Path={}".format(event.type, event.path))
                self.run_callback(callback)
                return True
            elif event.type == 'CREATED':
                logger.info("callback created: WatchedEvent_Type={}, WatchedEvent_Path={}".format(event.type, event.path))
                return True
            elif event.type == 'DELETED':
                logger.info("callback deleted: WatchedEvent_Type={}, WatchedEvent_Path={}".format(event.type, event.path))
                return False
            else:
                logger.error("callback failed: WatchedEvent_Type={}, WatchedEvent_Path={}".format(event.type, event.path))
                return True
        else:
            logging.error("handle exist callback failed: callback={}, WatchedEvent={}".format(callback, event))

    def start(self):
        logger.info("callback start")
        self.zk.start()
        self.watch()
        self.compensate()

    def shutdown(self):
        self.event.set()
        self.zk.close()

if __name__ == '__main__':
    zk_connect = '127.0.0.1:2181'
    zk_root = '/version_publish'
    callbacks_handler = Callback(zk_connect, zk_root)
    try:
        callbacks_handler.start()
    except KeyboardInterrupt:
        callbacks_handler.shutdown()
