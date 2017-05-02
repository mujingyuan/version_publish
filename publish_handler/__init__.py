from tornado.options import define, options
from tornado.web import Application, RequestHandler

from zk_handler.task_operation import ZkOperation

define('port', default=9101, type=int, help='server port')
define('bind', default='127.0.0.1', type=str, help='server bind')
define('zk_connect', default='127.0.0.1:2181', type=str, help='zookeeper connect')
define('zk_root', default='/version_publish', type=str, help='zookeeper root')


def make_app(handlers, **setting):
    app = Application(handlers, **setting)
    zk = ZkOperation(options.zk_connect, options.zk_root)
    setattr(app, 'zk', zk)
    return app

